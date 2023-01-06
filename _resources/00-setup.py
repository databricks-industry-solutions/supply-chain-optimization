# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

import os
import re 
import mlflow
db_prefix = "supply_chain_optimization"

# COMMAND ----------

# Get dbName and cloud_storage_path, reset and create database
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

dbName = db_prefix+"_"+current_user_no_at
cloud_storage_path = f"/Users/{current_user}/field_demos/{db_prefix}"
reset_all = dbutils.widgets.get("reset_all_data") == "true"

if reset_all:
  spark.sql(f"DROP DATABASE IF EXISTS {dbName} CASCADE")
  dbutils.fs.rm(cloud_storage_path, True)

spark.sql(f"""create database if not exists {dbName} LOCATION '{cloud_storage_path}/tables' """)
spark.sql(f"""USE {dbName}""")

# COMMAND ----------

print(cloud_storage_path)
print(dbName)

# COMMAND ----------

reset_all = dbutils.widgets.get('reset_all_data')
reset_all_bool = (reset_all == 'true')

# COMMAND ----------

path = cloud_storage_path

dirname = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
filename = "01-data-generator"
if (os.path.basename(dirname) != '_resources'):
  dirname = os.path.join(dirname,'_resources')
generate_data_notebook_path = os.path.join(dirname,filename)

def generate_data():
  dbutils.notebook.run(generate_data_notebook_path, 600, {"reset_all_data": reset_all, "dbName": dbName, "cloud_storage_path": cloud_storage_path})


if reset_all_bool:
  generate_data()
else:
  try:
    dbutils.fs.ls(path)
  except: 
    generate_data()

# COMMAND ----------

mlflow.set_experiment('/Users/{}/supply_chain_optimization'.format(current_user))

# COMMAND ----------


