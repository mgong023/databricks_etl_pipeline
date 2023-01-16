# Databricks notebook source
# MAGIC %run "./utils_manual"

# COMMAND ----------

import os
env = os.environ['ENV']
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {pipe_conf['l3_db']} LOCATION 'dbfs:/mnt/datalake/NBB/{env}/DATA/L3/iga/'")

# COMMAND ----------

ddl_create(pipe_conf)
