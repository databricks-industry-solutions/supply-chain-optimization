# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC 
# MAGIC 
# MAGIC **Situation**:
# MAGIC A manufacturer for power tools has 3 plants and delivers a set of 30 products to 5 distribution centers. Each distribution center is assigned to a set of between 40 and 60 hardware stores. Each store has a demand series for each of the products. 
# MAGIC 
# MAGIC 
# MAGIC **What's given**:
# MAGIC - We have the demand series for each product in each hardware store
# MAGIC - We have a mapping table that uniquely assigns each distribution center to a hardware store. This assumption is a simplification as it is possible that one hardware store obtains products from  different distribution centers
# MAGIC - We have a table that assignes the costs of shipping a product from each plant to each distribution center
# MAGIC - We have a table of the maximum qunatities that can be produced by and shipped from each plant to each of the distribution centers
# MAGIC 
# MAGIC **We proceeed in 2 steps**:
# MAGIC - *Demand Forecasting*: Derive next week's demand for each product and distribution center: 
# MAGIC   - For the demand series for each product within each store we generate a one-week-ahead forecast
# MAGIC   - For distribution center, we derive next week's estimated demand for each product
# MAGIC - *Minimization of transportation costs*: From the cost and constraints tables of producing by and shipping from a plant to a distribution center we derive cost-optimal transportation.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/databricks-industry-solutions/supply-chain-optimization/main/pictures/Plant_DistributionCenter_Store_Prouct_2.png?token=GHSAT0AAAAAAB46Q6LR4XK4XHT2HQVM44CQY5YKVBA" width=70%>

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=true
