# Databricks notebook source
import yaml
with open('../../config/config.yaml') as f:
    pipe_conf = yaml.load(f, Loader=yaml.FullLoader)
pipe_conf

# COMMAND ----------

def database_table_count(database_name: str):
    dftbls = sqlContext.sql("show tables")
    dfdbs = sqlContext.sql("show databases")
    for row in dfdbs.rdd.collect():
        tmp = "show tables from " + row['databaseName'] 
        if row['databaseName'] == 'default':
            dftbls = sqlContext.sql(tmp)
        else:
            dftbls = dftbls.union(sqlContext.sql(tmp))        
    dftbls = dftbls.where(f"database == '{database_name}'")        
    tmplist = []
    for row in dftbls.rdd.collect():
        try:
            tmp = 'select count(*) myrowcnt from ' + row['database'] + '.' + row['tableName']
            tmpdf = sqlContext.sql(tmp)
            myrowcnt= tmpdf.collect()[0]['myrowcnt'] 
            tmplist.append((row['database'], row['tableName'],myrowcnt))
        except:
            tmplist.append((row['database'], row['tableName'],-1))

    columns =  ['database', 'table/view_name', 'row_count']     
    df = spark.createDataFrame(tmplist, columns)    
    return df

# COMMAND ----------

def unit_testing(databricks_table: str, database: str, sql_server_table: str, columns: list, col_names_different=False):
    table = spark.table(databricks_table)
    dbs_table = table.rdd.takeSample(withReplacement=False,num=10, seed=42)
    dbs_table = spark.createDataFrame(dbs_table, schema=table.schema)
    dbs_columns = dbs_table.columns
    dbs_table = dbs_table.select(columns)

    sql_table = retrieve_sql_on_prem_data(database, sql_server_table)
    if col_names_different:
        # Order of columns should be the same in databricks as in sql server
        sql_table = sql_table.toDF(*dbs_columns).select(columns)
    else:
        sql_table = sql_table.select(columns)

    dbs_table.createOrReplaceTempView('dbs_table')
    sql_table.createOrReplaceTempView('sql_table')
    output_df = spark.sql("select * from dbs_table except select * from sql_table")
    return (output_df, output_df.count() == 0)

# COMMAND ----------

def full_testing(databricks_table: str, database: str, sql_server_table: str, columns: list, col_names_different=False):
    dbs_table = spark.table(databricks_table)
    dbs_columns = dbs_table.columns
    dbs_table = dbs_table.select(columns)
    
    sql_table = retrieve_sql_on_prem_data(database, sql_server_table)
    if col_names_different:
        # Order of columns should be the same in databricks as in sql server
        sql_table = sql_table.toDF(*dbs_columns).select(columns)
    else:
        sql_table = sql_table.select(columns)
    
    dbs_table.createOrReplaceTempView('dbs_table')
    sql_table.createOrReplaceTempView('sql_table')
    output_df = spark.sql("select * from dbs_table except select * from sql_table")
    return (output_df, output_df.count() == 0)
