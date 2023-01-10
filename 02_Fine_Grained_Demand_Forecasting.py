# Databricks notebook source
# MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/supply-chain-optimization

# COMMAND ----------

# MAGIC %md
# MAGIC # Fine Grained Demand Forecasting

# COMMAND ----------

# MAGIC %md
# MAGIC *Prerequisite: Make sure to run 01_Introduction_And_Setup before running this notebook.*
# MAGIC 
# MAGIC In this notebook we to a one-week-ahead forecast to estimate next week's demand for each store and product. We then aggregate on a distribution center level for each product.
# MAGIC 
# MAGIC Key highlights for this notebook:
# MAGIC - Use Databricks' collaborative and interactive notebook environment to find an appropriate time series mdoel
# MAGIC - Pandas UDFs (user-defined functions) can take your single-node data science code, and distribute it across different keys  

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

print(cloud_storage_path)
print(dbName)

# COMMAND ----------

import os
import datetime as dt
import numpy as np
import pandas as pd

from statsmodels.tsa.api import ExponentialSmoothing

import pyspark.sql.functions as f
from pyspark.sql.types import *

# COMMAND ----------

demand_df = spark.read.table(f"{dbName}.part_level_demand")
demand_df = demand_df.cache() # just for this example notebook

# COMMAND ----------

display(demand_df)

# COMMAND ----------

# This is just to create one example for development and testing
#example_product = demand_df.select("product").orderBy("product").limit(1).collect()[0].product
#example_store = demand_df.select("store").orderBy("store").limit(1).collect()[0].store
#pdf = demand_df.filter( (f.col("product") == example_product) & (f.col("store") == example_store)  ).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## One-step ahead forecast via Holt’s Winters Seasonal Method

# COMMAND ----------

def one_step_ahead_forecast(pdf: pd.DataFrame) -> pd.DataFrame:

    #Prepare seroes for forecast
    series_df = pd.Series(pdf['demand'].values, index=pdf['date'])
    series_df = series_df.asfreq(freq='W-MON')

    # One-step ahead forecast
    fit1 = ExponentialSmoothing(
        series_df,
        trend="add",
        seasonal="add",
        use_boxcox=False,
        initialization_method="estimated",
    ).fit(method="ls")
    fcast1 = fit1.forecast(1).rename("Additive trend and additive seasonal")

    # Collect Result
    df = pd.DataFrame(data = 
                      {
                         'product': pdf['product'].iloc[0], 
                         'store': pdf['store'].iloc[0], 
                         'date' : pd.to_datetime(series_df.index.values[-1]) + dt.timedelta(days=7), 
                         'demand' : int(abs(fcast1.iloc[-1]))
                      }, 
                          index = [0]
                     )
    return df

# COMMAND ----------

fc_schema = StructType(
  [
    StructField('product', StringType()),
    StructField('store', StringType()),
    StructField('date', DateType()),
    StructField('demand', FloatType())
  ]
)

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
n_tasks = demand_df.select("product", "store").distinct().count()

forecast_df = (
  demand_df
  .repartition(n_tasks, "product", "store")
  .groupBy("product", "store")
  .applyInPandas(one_step_ahead_forecast, schema=fc_schema)
)

display(forecast_df)

# COMMAND ----------

assert demand_df.select('product', 'store').distinct().count() == forecast_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate forecasts on distribution center level

# COMMAND ----------

distribution_center_to_store_mapping_table = spark.read.table(f"{dbName}.distribution_center_to_store_mapping_table")

# COMMAND ----------

display(distribution_center_to_store_mapping_table)

# COMMAND ----------

display(forecast_df)

# COMMAND ----------

distribution_center_demand = (
  forecast_df.
  join(distribution_center_to_store_mapping_table, [ "store" ] , "left").
  groupBy("distribution_center", "product").
  agg(f.sum("demand").alias("demand"))
)                              

# COMMAND ----------

display(distribution_center_demand)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to delta

# COMMAND ----------

distribution_center_demand_df_delta_path = os.path.join(cloud_storage_path, 'distribution_center_demand_df_delta')

# COMMAND ----------

# Write the data 
distribution_center_demand.write \
.mode("overwrite") \
.format("delta") \
.save(distribution_center_demand_df_delta_path)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {dbName}.distribution_center_demand")
spark.sql(f"CREATE TABLE {dbName}.distribution_center_demand USING DELTA LOCATION '{distribution_center_demand_df_delta_path}'")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {dbName}.distribution_center_demand"))

# COMMAND ----------

# MAGIC %md 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | pulp                                 | A python Linear Programming API      | https://github.com/coin-or/pulp/blob/master/LICENSE        | https://github.com/coin-or/pulp                      |
