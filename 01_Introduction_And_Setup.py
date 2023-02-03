# Databricks notebook source
# MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/supply-chain-optimization

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction
# MAGIC 
# MAGIC 
# MAGIC **Situation**:
# MAGIC The situation that we model is that of a manufacturer of power tools. The manufacturer operates 3 plants and delivers a set of 30 product SKUs to 5 distribution centers. Each distribution center is assigned to a set of between 40 and 60 hardware stores. All these parameters are treated as variables such that the pattern of the code may be scaled (see later). Each store has a demand series for each of the products. 
# MAGIC 
# MAGIC 
# MAGIC **The following are given**:
# MAGIC - the demand series for each product in each hardware store
# MAGIC - a mapping table that uniquely assigns each distribution center to a hardware store. This is a simplification as it is possible that one hardware store obtains products from different distribution centers.
# MAGIC - a table that assigns the costs of shipping a product from each manufacturing plant to each distribution center
# MAGIC - a table of the maximum quantities of product that can be produced and shipped from each plant to each of the distribution centers
# MAGIC 
# MAGIC 
# MAGIC **We proceed in 2 steps**:
# MAGIC - *Demand Forecasting*: The demand forecast is first estimated one week ahead. Aggregation yields the demand for each distribution center: 
# MAGIC   - For the demand series for each product within each store we generate a one-week-ahead forecast
# MAGIC   - For the distribution center, we derive next week's estimated demand for each product
# MAGIC - *Minimization of transportation costs*: From the cost and constraints tables of producing by and shipping from a plant to a distribution center we derive cost-optimal transportation.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/databricks-industry-solutions/supply-chain-optimization/main/pictures/Plant_DistributionCenter_Store_Prouct_2.png" width=70%>

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=true
