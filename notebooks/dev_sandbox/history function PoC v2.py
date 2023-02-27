# Databricks notebook source
from pyspark.sql.functions import *
import regex as re

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TARGET
# MAGIC CREATE OR REPLACE TABLE l3_iga.Products
# MAGIC (
# MAGIC    id INT,
# MAGIC    ProductName string,
# MAGIC    Rate float,
# MAGIC    VERSION_START date,
# MAGIC    VERSION_END date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC --Insert records into target table
# MAGIC INSERT OVERWRITE l3_iga.Products
# MAGIC VALUES
# MAGIC    (1, 'Tea', 7.00, '2022-10-11', null),
# MAGIC    (2, 'Tea', 5.00, '2021-12-31', '2022-10-11'),
# MAGIC    (3, 'Coffee', 20.00, '2021-12-31', null),
# MAGIC    (4, 'Biscuit', 40.00, '2021-12-31', null),
# MAGIC    (5, 'Pizza', 10.00, '2020-01-01', '1000-12-31'),
# MAGIC    (6, 'cookie', 2.00, '2023-01-20', null)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create source table
# MAGIC CREATE OR REPLACE TABLE l3_iga.UpdatedProducts
# MAGIC (
# MAGIC    ProductName string,
# MAGIC    Rate float
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC --Insert records into source table
# MAGIC INSERT OVERWRITE l3_iga.UpdatedProducts
# MAGIC VALUES
# MAGIC    ('Tea', 8.00), --updated
# MAGIC    ('Coffee', 20.00), --unchanged
# MAGIC    ('Biscuit', 45.00), --updated
# MAGIC    ('Applepie', 10.00) --new
# MAGIC    --cookie deleted

# COMMAND ----------

def history_scd2(target_table: str, source_table: str, key_columns: list, columns_changed: list, soft_delete=True):
    merge_condition = ''.join(f'TARGET.{key} = SOURCE.{key} AND ' for key in key_columns)[:-5]
    columns_changed_str = ''.join(f'TARGET.{key} <> SOURCE.{key} OR ' for key in columns_changed)[:-4]
    matched_condition = modify_matched_condition_scd2(target_table, source_table, columns_changed_str)
    spark.sql(f"CREATE OR REPLACE TEMP VIEW SOURCE AS SELECT -1 AS ID, *, current_date() AS VERSION_START, null AS VERSION_END FROM {source_table}")
    spark.sql(f"""
        MERGE INTO {target_table} TARGET
        USING SOURCE
        ON ({merge_condition})
        WHEN MATCHED AND ({matched_condition}) AND TARGET.VERSION_END is null
        THEN UPDATE SET TARGET.VERSION_END = current_date()
    """)
    spark.sql(f"""
        INSERT INTO {target_table}
        SELECT * FROM SOURCE WHERE NOT EXISTS(SELECT 1 FROM {target_table} WHERE NOT ({matched_condition.replace('TARGET', target_table)}))
    """)    
    if soft_delete:
        spark.sql(f"""
            UPDATE {target_table} TARGET SET VERSION_END = '1000-12-31'
            WHERE NOT EXISTS(SELECT * FROM SOURCE WHERE ({merge_condition}))
        """)
    print(f'Finished historizing {target_table}')

def modify_matched_condition_scd2(target_table: str, source_table: str, matched_condition: str):
    target = re.findall(r'TARGET.\w+', matched_condition)
    source = re.findall(r' SOURCE.\w+', matched_condition) # need whitespace in case 'SOURCE' is in a column name
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
        elif t_type == 'date' and s_type == 'date':
            res = "null"
        # create the null handling string
        string += f"(nvl({t}, {res}) <> nvl({s}, {res})) OR "
    return string[:-4] # remove last 'OR'

# COMMAND ----------

history_scd2(
    'l3_iga.Products',
    'l3_iga.UpdatedProducts',
    key_columns=['ProductName'],
    columns_changed=['ProductName', 'Rate'],
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from l3_iga.Products

# COMMAND ----------


