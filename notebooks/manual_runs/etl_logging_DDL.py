# Databricks notebook source
# MAGIC %run "./utils_manual"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {pipe_conf['l2_db']}.etl_logging(
  filedate date not null)
""")
