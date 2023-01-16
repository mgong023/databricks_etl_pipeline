# Databricks notebook source
#Last modified date: 10-Nov-22
#Author: Mihai
#Description:
import os
import regex as re
import yaml

from logger import Logger
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from typing import List
from datetime import date, timedelta, datetime

# COMMAND ----------

# Load pipeline config
with open(os.getcwd() + '/../config/config.yaml') as f:
    pipe_conf = yaml.load(f, Loader=yaml.FullLoader)

# COMMAND ----------

def sql_string_cleaner(query: str, remove_null=False):
    clean_string = query.replace('[', '').replace(']', '').replace('"', '').replace('+', '||')
    if remove_null is True:
        clean_string = clean_string.replace('NULL', '')
    print(clean_string)

# COMMAND ----------

def retrieve_sql_on_prem_data(db_name: str, table_name: str):
    user = "SA_NBB_INFDBR_P"
    domain = "NBB.LOCAL"
    pwd = os.environ['SA_NBB_INFDBR_P__SQPROB2H']

    database = db_name

    df = (
      spark.read.format("jdbc")
           .option("url", f"jdbc:sqlserver://SQPROB2H.NBB.LOCAL\PROB2H:1652;databaseName={database}")
           .option("dbtable", table_name)
           .option("user", user)
           .option("password", pwd)
           .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
           .load()
    )
    return df

# COMMAND ----------

def create_history_table(target_table: str, source_table: str, merge_condition: str, matched_condition: str, soft_delete=False, hard_delete=False):
    source_df = spark.table(source_table)
    source_df = source_df.withColumn("VERSION_START", current_date())
    source_df = source_df.withColumn("VERSION_END", to_date(lit('9999-12-31'), "yyyy-MM-dd"))
    source_df = source_df.withColumn("VERSION_LATEST", lit(1))
    if soft_delete:
        source_df = source_df.withColumn("IS_ACTIVE", lit(-1))
    source_df.createOrReplaceTempView('SOURCE')

    print("Running upsert...")
    spark.sql(f"""
        MERGE INTO {target_table} TARGET
        USING SOURCE
        ON ({merge_condition})
        --When records are matched, update the records if there is any change
        WHEN MATCHED AND ({matched_condition})
        THEN UPDATE SET TARGET.VERSION_LATEST = 0
        --When no records are matched, insert the incoming records from source table to target table
    """)
    spark.sql(f"""
        INSERT INTO {target_table}
        SELECT * FROM SOURCE WHERE NOT EXISTS(SELECT 1 FROM {target_table} WHERE NOT ({matched_condition.replace('TARGET', target_table)}))
    """)    
    if soft_delete:
        print("Running soft delete...")
        spark.sql(f"UPDATE {target_table} SET IS_ACTIVE = 1")
        spark.sql(f"""
            UPDATE {target_table} TARGET SET IS_ACTIVE = 0, VERSION_END = CURRENT_DATE()
            WHERE NOT EXISTS(SELECT * FROM SOURCE WHERE ({merge_condition}))
        """)
    elif hard_delete:
        print("Running hard delete...")
        spark.sql(f"""
            DELETE FROM {target_table} TARGET WHERE NOT EXISTS(SELECT * FROM SOURCE WHERE ({merge_condition}))
        """)
    spark.sql(f"UPDATE {target_table} SET VERSION_END = current_date WHERE (VERSION_END = '9999-12-31' AND VERSION_LATEST = 0)")
    print('Finished')

# COMMAND ----------

def modify_matched_condition(target_table: str, source_table: str, matched_condition: str):
    target = re.findall(r'TARGET.\w+', matched_condition)
    source = re.findall(r'SOURCE.\w+', matched_condition)
    target_types = dict(spark.table(target_table).dtypes)
    source_types = dict(spark.table(source_table).dtypes)
    string = ""
    for t, s in zip(target, source):
        # get column names
        t_col = re.findall(r'TARGET.\K\w+', t)
        s_col = re.findall(r'SOURCE.\K\w+', s)
        # retrieve the data type
        t_type = target_types[t_col[0]]
        s_type = source_types[s_col[0]]
        res = 0
        if t_type == 'string' and s_type == 'string':
            res = "''"
        # create the null handling string
        substring = f"(coalesce({t}, {res}) <> coalesce({s}, {res}))"
        string += substring + " OR "
    return string[:-4] # remove last 'OR'

# COMMAND ----------

# If exception is raised, L2 has not the data of today ingested
# If no exception is raised, L2 has data of today ingested
def ingestion_date_check(database: str,tables_to_exclude:list):
    tmp = "show tables from " + f"{database}"
    dftbls=sqlContext.sql(tmp)
    try:
        for row in dftbls.rdd.collect():
            if row['tableName'] in tables_to_exclude:
                continue
            else:
                tmp = f"select file_date from {row['database']}.{row['tableName']} where to_date(file_date) = CURRENT_DATE limit 1"  
            tmpdf = sqlContext.sql(tmp).collect()[0]['file_date']
    except:
        raise Exception(f"Table {row['tableName']} is missing today's data")

# COMMAND ----------

def dates_to_process(etl_logging: DataFrame, starting_date: datetime.date):
    today = date.today()
    try:
        last_etl_date = etl_logging.first()[0]
    except:
        # in case table is empty
        last_etl_date = starting_date
    delta = (today - last_etl_date).days

    # when data of dates are not processed
    if delta > 0:
        dates_to_be_processed = sorted([(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(delta+1) if delta > 0])
    else:
        dates_to_be_processed = [today.strftime('%Y-%m-%d')]
    return dates_to_be_processed

# COMMAND ----------

#if (yesterday-today)/yesterday > 0.1 or (yesterday-today)/yesterday < -0.1 then raise exception
def tolerance_check(database: str, table: str, date: str, volume_check:bool, tolerance: float, day: datetime.date):
    today = f"SELECT count (*) from {database}.{table} where to_date({date}) = '{day}'"
    yesterday = f"SELECT count (*) from {database}.{table} where to_date({date}) = date_sub('{day}',1)"
    today = sqlContext.sql(today).collect()[0][0]
    yesterday = sqlContext.sql(yesterday).collect()[0][0]
    if yesterday == 0:
        raise Exception ("There is no data for yesterday")
    elif ((volume_check) and (((yesterday-today)/yesterday > tolerance) or ((yesterday-today)/yesterday < -tolerance))):
        raise Exception ("Number of rows not within tolerance limit")
    print(f"Tolerance check is succesful for {table}.")

# COMMAND ----------

def retrieve_sql_on_prem_data_query(db_name: str, query: str):
    user = "SA_NBB_INFDBR_P"
    domain = "NBB.LOCAL"
    pwd = os.environ['SA_NBB_INFDBR_P__SQPROB2H']
    df = (
        spark.read.format("jdbc")
            .option("url", f"jdbc:sqlserver://SQPROB2H.NBB.LOCAL\PROB2H:1652;databaseName={db_name}")
            .option("user", user)
            .option("password", pwd)
            .option("query", query)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
    )
    return df

# COMMAND ----------

def upsert(target_table, source_table, merge_condition):
    spark.sql(f"""
        MERGE INTO {target_table} TARGET
        USING {source_table} SOURCE
        ON ({merge_condition})
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

def generate_row_number(id_column_name: str, table: str):
    df = spark.table(f"{pipe_conf['l3_db']}.{table}")
    w = Window().partitionBy(id_column_name).orderBy(lit('A'))
    df = df.withColumn(id_column_name, row_number().over(w))
    df.write.mode("overwrite").saveAsTable(f"{pipe_conf['l3_db']}.{table}")
