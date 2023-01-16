# Databricks notebook source
import yaml

# COMMAND ----------

# Load pipeline config
with open('../../config/config.yaml') as f:
    pipe_conf = yaml.load(f, Loader=yaml.FullLoader)

pipe_conf

# COMMAND ----------

def ddl_create(config: dict):
    for table, value in config['tables'].items():
        query = f"CREATE OR REPLACE TABLE {config['l3_db']}.{table}("
        for k, v in value['columns'].items():        
            query += f'{k} {v},'
        query = query[:-1] + ')'
        try:
            spark.sql(query)
            print(f"{table} is created successfully")
        except Exception as exception:
            print("Exception message: {}".format(exception))
            raise Exception(f"An error occcured in {table}")

# COMMAND ----------

def refresh_db(config: dict, on_prem_db_name: str=None, refill: bool=False):
    if config['refresh_db']:
        for table, value in config['tables'].items():
            print(f'Truncating table {table}')
            spark.sql(f"TRUNCATE TABLE {config['l3_db']}.{table}")
            if refill:
                print(f'Refilling table {table}')
                columns = ','.join(value['columns'].keys())
                df = retrieve_sql_on_prem_data_query(on_prem_db_name, f"SELECT {columns} FROM [{on_prem_db_name}].[IC].[{value['on_prem']}]")
                df.createOrReplaceTempView("tmpTable") 
                spark.sql(f"INSERT INTO {config['l3_db']}.{table} SELECT * FROM tmpTable")
    else:
        print('truncate_db is set to false')

# COMMAND ----------

def refresh_tables(config: dict, on_prem_db_name: str=None, refill: bool=False):
    tables = config['refresh_tables']
    if tables == [None]:
        print('No tables specified to truncate')
    else:
        for table, value in config['tables'].items():
            if table in tables:
                print(f'Truncating table {table}')
                spark.sql(f"TRUNCATE TABLE {config['l3_db']}.{table}")
                if refill:
                    print(f'Refilling table {table}')
                    columns = ','.join(value['columns'].keys())
                    df = retrieve_sql_on_prem_data_query(on_prem_db_name, f"SELECT {columns} FROM [{on_prem_db_name}].[IC].[{value['on_prem']}]")
                    df.createOrReplaceTempView("tmpTable")
                    spark.sql(f"INSERT INTO {config['l3_db']}.{table} SELECT * FROM tmpTable")

# COMMAND ----------


