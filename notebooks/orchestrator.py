# Databricks notebook source
# MAGIC %run "./utils"

# COMMAND ----------

with open('../config/config.yaml') as f:
    pipe_conf = yaml.load(f, Loader=yaml.FullLoader)
pipe_conf

# COMMAND ----------

from pyspark.sql.types import StructType
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # L2 checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data ingestion check

# COMMAND ----------

ingestion_date_check(pipe_conf['l2_db'],['etl_logging'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tolerance check

# COMMAND ----------

tablescheck = [
    'nbbentitlementreport',
    'nbbidentityreport',
    'nbbbundlereport'
]

# COMMAND ----------

# MAGIC %md
# MAGIC # Tables creation

# COMMAND ----------

dbutils.notebook.run('manual_runs/etl_logging_DDL', 0, {})
dbutils.notebook.run('manual_runs/tables_DDL', 0, {})

# COMMAND ----------

# MAGIC %md
# MAGIC # Orchestrator

# COMMAND ----------

df_etl_logging = spark.table(f"{pipe_conf['l2_db']}.etl_logging").orderBy(col('filedate').desc())
df_etl_logging.display()

# COMMAND ----------

dates_to_be_processed = dates_to_process(df_etl_logging, starting_date=pipe_conf['etl_start_date'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL

# COMMAND ----------

for day in dates_to_be_processed:
    try:
        # run all ETL notebooks
        pipe_conf['date'] = day
        for table in tablescheck:
            tolerance_check(pipe_conf['l2_db'], table, 'file_date',pipe_conf['VolumeCheck'], pipe_conf['PctVariation_Tolerance'] , day)
        dbutils.notebook.run('ETL_notebook1', 0, pipe_conf)
        # Lastly, views creation
        spark.sql(f"INSERT INTO {pipe_conf['l2_db']}.etl_logging SELECT '{day}'")
    except:
        raise Exception('One of ETLs or checks failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views

# COMMAND ----------

dbutils.notebook.run('manual_runs/views_DDL', 0, {})
