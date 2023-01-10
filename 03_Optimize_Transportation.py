# Databricks notebook source
# MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/supply-chain-optimization

# COMMAND ----------

# MAGIC %md
# MAGIC # Transport Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC *Prerequisite: Make sure to run 02_Fine_Grained_Demand_Forecasting before running this notebook.*
# MAGIC 
# MAGIC In this notebook we solve the LP to optimize transport costs when shipping products from the plants to the distribution centers. Furthermore, we show how to scale to hunderd thousands of products.
# MAGIC 
# MAGIC Key highlights for this notebook:
# MAGIC - Use Databricks' collaborative and interactive notebook environment to find optimization procedure
# MAGIC - Pandas UDFs (user-defined functions) can take your single-node data science code, and distribute it across different keys 
# MAGIC 
# MAGIC More precisely we solve the following optimzation problem for each product.
# MAGIC 
# MAGIC *Mathematical goal:*
# MAGIC We have a set of plants that distribute products to a set of distribution centers. The goal is to minimize overall shipment costs, i.e. we minimize w.r.t. quantities: <br/>
# MAGIC cost_of_plant_1_to_distribution_center_1 * quantity_shipped_of_plant_1_to_distribution_center_1 <br/> \+ … \+ <br/>
# MAGIC cost_of_plant_1_to_distribution_center_n * quantity_shipped_of_plant_n_to_distribution_center_m 
# MAGIC 
# MAGIC *Mathematical constraints:*
# MAGIC - quantities shipped must be zero or positive integers
# MAGIC - summing up the quantities shipped from one plant does not exceed the maximum supply of the plant for each product
# MAGIC - summing up the quantities shipped to each distribution center meets at least the demand as forecasted in the previous notebook

# COMMAND ----------

# The pulp library is used for solving the LP problem
# We used this documentation for developemnt of this notebook
# https://coin-or.github.io/pulp/CaseStudies/a_transportation_problem.html
%pip install pulp

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

print(cloud_storage_path)
print(dbName)

# COMMAND ----------

import os
import datetime as dt
import re
import numpy as np
import pandas as pd

import pulp

import pyspark.sql.functions as f
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining and solving the LP

# COMMAND ----------

# Demand for each distribution center, one line per product
distribution_center_demand = spark.read.table(f"{dbName}.distribution_center_demand")
distribution_center_demand = distribution_center_demand.groupBy("Product").pivot("distribution_center").agg(f.first("demand").alias("demand"))
for name in distribution_center_demand.schema.names:
  distribution_center_demand = distribution_center_demand.withColumnRenamed(name, name.replace("Distribution_Center", "Demand_Distribution_Center"))
distribution_center_demand = distribution_center_demand.withColumnRenamed("Product", "Product".replace("Product", "product")).sort("product")
display(distribution_center_demand)

# COMMAND ----------

# Plant supply, one line per product
plant_supply = spark.read.table(f"{dbName}.supply_table")
for name in plant_supply.schema.names:
  plant_supply = plant_supply.withColumnRenamed(name, name.replace("plant", "Supply_Plant"))
plant_supply = plant_supply.sort("product")
display(plant_supply)

# COMMAND ----------

# Transportation cost table, one, line per product and plant
transport_cost_table = spark.read.table(f"{dbName}.transport_cost_table")
for name in transport_cost_table.schema.names:
  transport_cost_table = transport_cost_table.withColumnRenamed(name, name.replace("Distribution_Center", "Cost_Distribution_Center"))
display(transport_cost_table)

# COMMAND ----------

# Create a table with all information to iterate over. The table has one row per product and plant, with column-wise
# - The costs to ship from plant (rowwise) to distribution center (columnwise)
# - The supply restrictions for each product (rowwise) from each plant (columnwise)
# - The demand resctrictions for each product (rowwise) to mee the demand of each distribution center (columnwise)
lp_table_all_info = (transport_cost_table.
                     join(plant_supply, ["product"], how="inner").
                     join(distribution_center_demand, ["product"], how="inner")
                    )
display(lp_table_all_info)

# COMMAND ----------

# Define output schema of final result table
res_schema = StructType(
  [
    StructField('product', StringType()),
    StructField('plant', StringType()),
    StructField('distribution_center', StringType()),
    StructField('qty_shipped', IntegerType())
  ]
)

# COMMAND ----------

# Define a function that solves the LP for one product
def transport_optimization(pdf: pd.DataFrame) -> pd.DataFrame:

  #Plants list, this defines the order of other data structures related to plants
  plants_lst = sorted(pdf["plant"].unique().tolist())

  # Distribution center list, this defines the order of other data structures related to distribution centers
  p = re.compile('^Cost_(Distribution_Center_\d)$')
  distribution_centers_lst = sorted([ s[5:] for s in list(pdf.columns.values) if p.search(s) ])

  # Define all possible routes
  routes = [(p, d) for p in plants_lst for d in distribution_centers_lst]

  # Create a dictionary which contains the LP variables. The reference keys to the dictionary are the plant's name, then the distribution center's name and the
  # data is Route_Tuple. (e.g. ["plant_1"]["distribution_center_1"]: Route_plant_1_distribution_center_1). Set lower limit to zero, upper limit to None and
  # define as integers
  vars = pulp.LpVariable.dicts("Route", (plants_lst, distribution_centers_lst), 0, None, pulp.LpInteger)

  # Subset other lookup tables
  ss_prod = pdf[ "product" ][0]

  # Costs, order of distribution centers and plants matter
  transport_cost_table_pdf = pdf.filter(regex="^Cost_Distribution_Center_\d+$|^plant$")
  transport_cost_table_pdf = (transport_cost_table_pdf.
                              rename(columns=lambda x: re.sub("^Cost_Distribution_Center","Distribution_Center",x)).
                              set_index("plant").
                              reindex(plants_lst, axis=0).
                              reindex(distribution_centers_lst, axis=1)
                             )
  costs = pulp.makeDict([plants_lst, distribution_centers_lst], transport_cost_table_pdf.values.tolist(), 0)

  # Supply, order of plants matters
  plant_supply_pdf = (pdf.
                      filter(regex="^Supply_Plant_\d+$").
                      drop_duplicates().
                      rename(columns=lambda x: re.sub("^Supply_Plant","plant",x)).
                      reindex(plants_lst, axis=1)
                     )

  supply = plant_supply_pdf.to_dict("records")[0]

  # Demand, order of distribution centers matters
  distribution_center_demand_pdf =  (pdf.
                      filter(regex="^Demand_Distribution_Center_\d+$").
                      drop_duplicates().
                      rename(columns=lambda x: re.sub("^Demand_Distribution_Center","Distribution_Center",x)).
                      reindex(distribution_centers_lst, axis=1)
                     )

  demand = distribution_center_demand_pdf.to_dict("records")[0]

  # Create the 'prob' variable to contain the problem data
  prob = pulp.LpProblem("Product_Distribution_Problem", pulp.LpMinimize)

  # Add objective function to 'prob' first
  prob += (
      pulp.lpSum([vars[p][d] * costs[p][d] for (p, d) in routes]),
      "Sum_of_Transporting_Costs",
  )

  # Add supply restrictions
  for p in plants_lst:
      prob += (
          pulp.lpSum([vars[p][d] for d in distribution_centers_lst]) <= supply[p],
          f"Sum_of_Products_out_of_Plant_{p}",
      )

  # Add demand restrictions
  for d in distribution_centers_lst:
      prob += (
          pulp.lpSum([vars[p][d] for p in plants_lst]) >= demand[d],
          f"Sum_of_Products_into_Distibution_Center{d}",
      )

  # The problem is solved using PuLP's choice of Solver
  prob.solve()

  # Write output fot the product
  if (pulp.LpStatus[prob.status] == "Optimal"):
    name_lst = [ ]
    value_lst = [ ]
    for v in prob.variables():
      name_lst.append(v.name) 
      value_lst.append(v.varValue)
      res = pd.DataFrame(data={'name': name_lst, 'qty_shipped': value_lst})
      res[ "qty_shipped" ] = res[ "qty_shipped" ].astype("int")
      res[ "plant" ] =  res[ "name" ].str.extract(r'(plant_\d+)')
      res[ "distribution_center" ] =  res[ "name" ].str.extract(r'(Distribution_Center_\d+)')
      res[ "product" ] = ss_prod
      res = res.drop("name", axis = 1)
      res = res[[ "product", "plant", "distribution_center", "qty_shipped"]]
  else:
      res = pd.DataFrame(data= {  "product" : [ ss_prod ] , "plant" : [ None ], "distribution_center" : [ None ], "qty_shipped" : [ None ]})
  return res

# COMMAND ----------

# Test the function
#product_selection = "nail_1"
# pdf = lp_table_all_info.filter(f.col("product")==product_selection).toPandas()
# transport_optimization(pdf)

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
n_tasks = lp_table_all_info.select("product").distinct().count()

optimal_transport_df = (
  lp_table_all_info
  .repartition(n_tasks, "product")
  .groupBy("product")
  .applyInPandas(transport_optimization, schema=res_schema)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to delta

# COMMAND ----------

shipment_recommendations_df_delta_path = os.path.join(cloud_storage_path, 'shipment_recommendations_df_delta')

# COMMAND ----------

# Write the data 
optimal_transport_df.write \
.mode("overwrite") \
.format("delta") \
.save(shipment_recommendations_df_delta_path)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {dbName}.shipment_recommendations")
spark.sql(f"CREATE TABLE {dbName}.shipment_recommendations USING DELTA LOCATION '{shipment_recommendations_df_delta_path}'")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {dbName}.shipment_recommendations"))

# COMMAND ----------

# MAGIC %md 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | pulp                                 | A python Linear Programming API      | https://github.com/coin-or/pulp/blob/master/LICENSE        | https://github.com/coin-or/pulp                      |
