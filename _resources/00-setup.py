# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

import os
import re 
import mlflow
db_prefix = "supply_chain_optimization"

# COMMAND ----------

dbName = "sco_data_james"
catalogPrefix = "supply_chain_optimization_catalog"

# COMMAND ----------

# Get dbName and cloud_storage_path, reset and create database
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

catalogName = catalogPrefix+"_"+current_user_no_at

reset_all = dbutils.widgets.get("reset_all_data") == "true"

if reset_all:
  spark.sql(f"DROP CATALOG IF EXISTS {catalogName} CASCADE")

spark.sql(f"""create catalog if not exists {catalogName}""")
spark.sql(f"""USE CATALOG {catalogName}""")
spark.sql(f"""create database if not exists {dbName}""")
spark.sql(f"""USE {dbName}""")

# COMMAND ----------

print(f"The catalog {catalogName} will be used")
print(f"The database {dbName} will be used")

# COMMAND ----------

reset_all = dbutils.widgets.get('reset_all_data')
reset_all_bool = (reset_all == 'true')

# COMMAND ----------

dirname = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
filename = "01-data-generator"
if (os.path.basename(dirname) != '_resources'):
  dirname = os.path.join(dirname,'_resources')

generate_data_notebook_path = os.path.join(dirname,filename)

# print(generate_data_notebook_path)

def generate_data():
  dbutils.notebook.run(generate_data_notebook_path, 3000, {"reset_all_data": reset_all, "catalogName": catalogName,   "dbName": dbName})

# COMMAND ----------

if reset_all_bool:
  generate_data()

# COMMAND ----------

mlflow.set_experiment('/Users/{}/supply_chain_optimization'.format(current_user))

# COMMAND ----------


