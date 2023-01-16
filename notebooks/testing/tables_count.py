# Databricks notebook source
# MAGIC %run "./test_functions"

# COMMAND ----------

df = database_table_count('l3_iga')
df.display()

# COMMAND ----------


