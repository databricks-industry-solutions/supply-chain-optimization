# Databricks notebook source
dbutils.widgets.dropdown('reset_all_data', 'false', ['true', 'false'], 'Reset all data')
dbutils.widgets.text('dbName',  'supply_chain_optimization_max_kohler' , 'Database Name')
dbutils.widgets.text('cloud_storage_path',  '/Users/max.kohler@databricks.com/field_demos/supply_chain_optimization', 'Storage Path')

# COMMAND ----------

print("Starting ./_resources/01-data-generator")

# COMMAND ----------

cloud_storage_path = dbutils.widgets.get('cloud_storage_path')
dbName = dbutils.widgets.get('dbName')
reset_all_data = dbutils.widgets.get('reset_all_data') == 'true'

# COMMAND ----------

print(cloud_storage_path)
print(dbName)
print(reset_all_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Packages

# COMMAND ----------

import pandas as pd
import numpy as np
import datetime

from dateutil.relativedelta import relativedelta
from dateutil import rrule

import os
import string
import random

import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window

import statsmodels.api as sm
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate demand series data

# COMMAND ----------

# MAGIC %md
# MAGIC Parameters

# COMMAND ----------

n=3 # Number of replicates per product category
ts_length_in_weeks = 104 # Length of a time series in weeks
number_of_stores = 30
n_distribution_centers = 5
n_plants = 3 # Number of plants

# COMMAND ----------

# MAGIC %md
# MAGIC Create a Product Table

# COMMAND ----------

ProdCatSchema = StructType([       
    StructField('product_categories', StringType(), True),
    StructField('transport_baseline_cost', FloatType(), True)
])

category_data = [("drilling machine", 0.72),("cordless screwdriver", 0.93),("impact drill", 1.11),("current meter", 0.71),("hammer", 0.97),("screwdriver",1.01) , ("nail", 0.91)  ,("screw", 0.61) ,("spirit level", 0.81),("toolbox", 1.31)]

products_categories = spark.createDataFrame(data=category_data, schema = ProdCatSchema)

products_versions = spark.createDataFrame(
  list(range(1,(n+1))),
  StringType()).toDF("product_versions")

product_table = (
  products_categories.
  crossJoin(products_versions).
  select(f.concat_ws('_', f.col("product_categories"), f.col("product_versions")).alias("product"), f.col("transport_baseline_cost"))
                )

display(product_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC Introduce Stores

# COMMAND ----------

store_table = spark.createDataFrame(
  list(range(1,(number_of_stores+1))),
  StringType()).toDF("stores_number")

store_table = store_table.select(f.concat_ws('_',f.lit("Store"), f.col("stores_number")).alias("store"))

display(store_table)

# COMMAND ----------

products_in_stores_table = (
  product_table.
  crossJoin(store_table)
)
display(products_in_stores_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC Generate Date Series

# COMMAND ----------

# End Date: Monday of the current week
end_date = datetime.datetime.now().replace(hour=0, minute=0, second= 0, microsecond=0) 
end_date = end_date + datetime.timedelta(-end_date.weekday()) #Make sure to get the monday before

# Start date: Is a monday, since we will go back integer number of weeks
start_date = end_date + relativedelta(weeks= (- ts_length_in_weeks))

# Make a sequence 
date_range = list(rrule.rrule(rrule.WEEKLY, dtstart=start_date, until=end_date))

#Create a pandas data frame
date_range = pd.DataFrame(date_range, columns =['date'])

display(date_range)

# COMMAND ----------

# MAGIC %md
# MAGIC Simulate parameters for ARMA series

# COMMAND ----------

# Define schema for new columns
arma_schema = StructType(
  [
    StructField("Variance_RN", FloatType(), True),
    StructField("Offset_RN", FloatType(), True),
    StructField("AR_Pars_RN", ArrayType(FloatType()), True),
    StructField("MA_Pars_RN", ArrayType(FloatType()), True)
  ]
)

# Generate random numbers for the ARMA process
np.random.seed(123)
n_ = products_in_stores_table.count()


variance_random_number = list(abs(np.random.normal(10, 2, n_)))
offset_random_number = list(np.maximum(abs(np.random.normal(100, 50, n_)), 30))
ar_length_random_number = np.random.choice(list(range(1,4)), n_)
ar_parameters_random_number = [np.random.uniform(low=0.1, high=0.3, size=x) for x in ar_length_random_number] 
ma_length_random_number = np.random.choice(list(range(1,4)), n_)
ma_parameters_random_number = [np.random.uniform(low=0.1, high=0.3, size=x) for x in ma_length_random_number] 


# Collect in a dataframe
pdf_helper = (pd.DataFrame(variance_random_number, columns =['Variance_RN']). 
              assign(Offset_RN = offset_random_number).
              assign(AR_Pars_RN = ar_parameters_random_number).
              assign(MA_Pars_RN = ma_parameters_random_number) 
             )

spark_df_helper = spark.createDataFrame(pdf_helper, schema=arma_schema)
spark_df_helper = (spark_df_helper.
  withColumn("row_id", f.monotonically_increasing_id()).
  withColumn('row_num', f.row_number().over(Window.orderBy('row_id'))).
  drop(f.col("row_id"))
                  )

products_in_stores_table = (products_in_stores_table.
                            withColumn("row_id", f.monotonically_increasing_id()).
                            withColumn('row_num', f.row_number().over(Window.orderBy('row_id'))).
                            drop(f.col("row_id"))
                           )


products_in_stores_table = products_in_stores_table.join(spark_df_helper, ("row_num")).drop(f.col("row_num"))
display(products_in_stores_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC Generate individual demand series

# COMMAND ----------

# To maximize parallelism, we can allocate each ("product", store") group its own Spark task.
# We can achieve this by:
# - disabling Adaptive Query Execution (AQE) just for this step
# - partitioning our input Spark DataFrame as follows:
spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
n_tasks = products_in_stores_table.select("product", "store").distinct().count()


# function to generate an ARMA process
def generate_arma(arparams, maparams, var, offset, number_of_points, plot):
  np.random.seed(123)
  ar = np.r_[1, arparams] 
  ma = np.r_[1, maparams] 
  y = sm.tsa.arma_generate_sample(ar, ma, number_of_points, scale=var, burnin= 1) + offset
  y = np.round(y).astype(int)
  y = np.absolute(y)
  
  if plot:
    x = np.arange(1, len(y) +1)
    plt.plot(x, y, color ="red")
    plt.show()
    
  return(y)


#Schema for output dataframe
schema = StructType(  
                    [
                      StructField("product", StringType(), True),
                      StructField("store", StringType(), True),
                      StructField("date", DateType(), True),
                      StructField("demand", FloatType(), True),
                      StructField("row_number", FloatType(), True)
                    ])

# Generate an ARMA
def time_series_generator_pandas_udf(pdf):
  out_df = date_range.assign(
   demand = generate_arma(arparams = pdf.AR_Pars_RN.iloc[0], 
                        maparams= pdf.MA_Pars_RN.iloc[0], 
                        var = pdf.Variance_RN.iloc[0], 
                        offset = pdf.Offset_RN.iloc[0], 
                        number_of_points = date_range.shape[0], 
                        plot = False),
  product = pdf["product"].iloc[0],
  store = pdf["store"].iloc[0]
    
  )
  
  out_df["row_number"] = range(0,len(out_df))
  
  out_df = out_df[["product", "store", "date", "demand", "row_number"]]

  return(out_df)

#pdf = products_in_stores_table.toPandas().head(1)

# Apply the Pandas UDF and clean up
demand_df = ( 
  products_in_stores_table.
   #repartition(n_tasks, "product", "store").
   groupby("product", "store"). 
   applyInPandas(time_series_generator_pandas_udf, schema).
   select("product", "store", "date", "demand")
)

#assert date_range.shape[0] * products_in_stores_table.count() == demand_df.count()

display(demand_df)

# COMMAND ----------

# Test if demand is in a realistic range
#display(demand_df.groupBy("product", "store").mean("demand"))

# COMMAND ----------

# Select a sepecific time series
# display(demand_df.join(demand_df.sample(False, 1 / demand_df.count(), seed=0).limit(1).select("product", "store"), on=["product", "store"], how="inner"))

# COMMAND ----------

# MAGIC %md
# MAGIC Save as a Delta table

# COMMAND ----------

demand_df_delta_path = os.path.join(cloud_storage_path, 'demand_df_delta')

# COMMAND ----------

# Write the data 
demand_df.write \
.mode("overwrite") \
.format("delta") \
.save(demand_df_delta_path)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {dbName}.part_level_demand")
spark.sql(f"CREATE TABLE {dbName}.part_level_demand USING DELTA LOCATION '{demand_df_delta_path}'")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {dbName}.part_level_demand"))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) as row_count FROM {dbName}.part_level_demand"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate hardware to distribution center mapping table

# COMMAND ----------

distribution_centers = (
  spark.createDataFrame(list(range(1, n_distribution_centers + 1)),StringType()).
  toDF("distribution_center_helper").
  withColumn("distribution_center", f.concat_ws('_', f.lit("Distribution_Center"), f.col("distribution_center_helper"))).
  select("distribution_center")
)


display(distribution_centers)

# COMMAND ----------

# We need more distribution centers than stores
assert (distribution_centers.count() <= store_table.count()) & (distribution_centers.count() > 0)

#Replicate distribution centers such that all distribution centers are used, but the table has the same number of rows than store_table
divmod_res = divmod(store_table.count(), distribution_centers.count())

rest_helper = distribution_centers.limit(divmod_res[1])
maximum_integer_divisor = (
  spark.createDataFrame(list(range(1, divmod_res[0] + 1)),StringType()).
  toDF("number_helper").
  crossJoin(distribution_centers).
  select("distribution_center")
)

distribution_centers_replicated = maximum_integer_divisor.unionAll(rest_helper)

assert distribution_centers_replicated.count() == store_table.count()

# Append distribution_centers_replicated and store_table column-wise
distribution_centers_replicated = (distribution_centers_replicated.
  withColumn("row_id", f.monotonically_increasing_id()).
  withColumn('row_num', f.row_number().over(Window.orderBy('row_id'))).
  drop(f.col("row_id"))
                  )

store_table = (store_table.
                            withColumn("row_id", f.monotonically_increasing_id()).
                            withColumn('row_num', f.row_number().over(Window.orderBy('row_id'))).
                            drop(f.col("row_id"))
                           )


distribution_center_to_store_mapping_table = store_table.join(distribution_centers_replicated, ("row_num")).drop(f.col("row_num"))
store_table = store_table.drop(f.col("row_num"))
distribution_centers_replicated = distribution_centers_replicated.drop(f.col("row_num"))

display(distribution_center_to_store_mapping_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Save as a Delta table

# COMMAND ----------

distribution_center_to_store_mapping_delta_path = os.path.join(cloud_storage_path, 'distribution_center_to_store_mapping')

# COMMAND ----------

# Write the data 
distribution_center_to_store_mapping_table.write \
.mode("overwrite") \
.format("delta") \
.save(distribution_center_to_store_mapping_delta_path)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {dbName}.distribution_center_to_store_mapping_table")
spark.sql(f"CREATE TABLE {dbName}.distribution_center_to_store_mapping_table USING DELTA LOCATION '{distribution_center_to_store_mapping_delta_path}'")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {dbName}.distribution_center_to_store_mapping_table"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate a transport cost table for each plant to each ditribution center for each product

# COMMAND ----------

baseline_costs = products_in_stores_table.select("product", "transport_baseline_cost" ).distinct()
display(baseline_costs)

# COMMAND ----------

plants_lst = ["plant_" + str(i) for i in  range(1,n_plants+1)]
plants_df = spark.createDataFrame([(p,) for p in plants_lst], ['plant'])
display(plants_df)

# COMMAND ----------

tmp_map_distribution_center_to_store = spark.read.table(f"{dbName}.distribution_center_to_store_mapping_table")
distribution_center_df = (spark.read.table(f"{dbName}.part_level_demand").
                          select("product","store").
                          join(tmp_map_distribution_center_to_store, ["store"],  how="inner").
                          select("product","distribution_center").
                          distinct()
                         )
distribution_center_df = distribution_center_df.join(baseline_costs, ["product"],  how="inner")
display(distribution_center_df)

# COMMAND ----------

plants_to_distribution_centers = plants_df.crossJoin(distribution_center_df)
display(plants_to_distribution_centers)

# COMMAND ----------

# For testing
#pdf = plants_to_distribution_centers.filter( (f.col("plant") == "plant_1") & (f.col("product") == "drilling machine_1")).toPandas()

# COMMAND ----------

def cost_generator(pdf: pd.DataFrame) -> pd.DataFrame:
  pdf_return = pdf.assign(transprot_cost_variation =  np.random.uniform(low=1.1, high=2.0, size=len(pdf)))
  pdf_return["transport_cost"] = pdf_return["transport_baseline_cost"] * pdf_return["transprot_cost_variation"]
  pdf_return = pdf_return[[ "plant", "product", "distribution_center", "transport_cost"]]
  return pdf_return

# COMMAND ----------

cost_schema = StructType(
  [
    StructField('plant', StringType()),
    StructField('product', StringType()),
    StructField('distribution_center', StringType()),
    StructField('transport_cost', FloatType())
  ]
)

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
n_tasks = plants_to_distribution_centers.select("plant", "product").distinct().count()

transport_cost_table = (
  plants_to_distribution_centers
  .repartition(n_tasks, "plant", "product")
  .groupBy("plant", "product")
  .applyInPandas(cost_generator, schema=cost_schema)
)

display(transport_cost_table)

# COMMAND ----------

transport_cost_table = (transport_cost_table.
                        groupBy("plant", "product").
                        pivot("distribution_center").
                        agg(f.first("transport_cost")).orderBy("product", "plant")
                       )
display(transport_cost_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Save as a Delta table

# COMMAND ----------

cost_table_delta_path = os.path.join(cloud_storage_path, 'cost_table')

# COMMAND ----------

# Write the data 
transport_cost_table.write \
.mode("overwrite") \
.format("delta") \
.save(cost_table_delta_path)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {dbName}.transport_cost_table")
spark.sql(f"CREATE TABLE {dbName}.transport_cost_table USING DELTA LOCATION '{cost_table_delta_path}'")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {dbName}.transport_cost_table"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate a maximum supply table for each plant and product

# COMMAND ----------

# Create a list with all plants
all_plants = spark.read.table(f"{dbName}.transport_cost_table").select("plant").distinct().collect()
all_plants = [row[0] for row in all_plants]

# Create a list with fractions: Sum must be larger than one to fullfill the demands
fractions_lst = [round(random.uniform(0.4, 0.8),1) for x in all_plants[1:]]
fractions_lst.append(max( 0.4,  1 - sum(fractions_lst)))

# Combine to a dictionary
plant_supply_in_percentage_of_demand = {all_plants[i]: fractions_lst[i] for i in range(len(all_plants))}

#Get maximum demand in history and sum up the demand of all distribution centers
map_store_to_dc_tmp = spark.read.table(f"{dbName}.distribution_center_to_store_mapping_table")
max_demands_per_dc = (spark.read.table(f"{dbName}.part_level_demand").
                      groupBy("product", "store").
                      agg(f.max("demand").alias("demand")).
                      join(map_store_to_dc_tmp, ["store"], how = "inner"). # This join will not produce duplicates, as one store is assigned to exactly one distribution center
                      groupBy("product").
                      agg(f.sum("demand").alias("demand"))   
                      ) 
# Distribute parts of the demands per product to the plants
for item in plant_supply_in_percentage_of_demand.items():
  max_demands_per_dc = max_demands_per_dc.withColumn(item[0], f.ceil(item[1] * f.col("demand")))

# This table must be saved in Delta later  
plant_supply = max_demands_per_dc.select("product", *all_plants).sort("product")
#display(plant_supply)

# COMMAND ----------

display(spark.read.table(f"{dbName}.distribution_center_to_store_mapping_table"))

# COMMAND ----------

display(spark.read.table(f"{dbName}.part_level_demand"))

# COMMAND ----------

# MAGIC %md
# MAGIC Save as a Delta table

# COMMAND ----------

supply_table_delta_path = os.path.join(cloud_storage_path, 'supply_table')

# COMMAND ----------

# Write the data 
plant_supply.write \
.mode("overwrite") \
.format("delta") \
.save(supply_table_delta_path)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {dbName}.supply_table")
spark.sql(f"CREATE TABLE {dbName}.supply_table USING DELTA LOCATION '{supply_table_delta_path}'")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {dbName}.supply_table"))

# COMMAND ----------

print("Ending ./_resources/01-data-generator")
