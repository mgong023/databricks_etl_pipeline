# Databricks notebook source
from pyspark.sql.functions import *
import regex as re

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TARGET
# MAGIC CREATE OR REPLACE TABLE l3_iga.Products
# MAGIC (
# MAGIC    ProductID INT,
# MAGIC    ProductName string,
# MAGIC    Rate float,
# MAGIC    VERSION_START date,
# MAGIC    VERSION_END date,
# MAGIC    VERSION_LATEST integer,
# MAGIC    IS_ACTIVE integer
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC --Insert records into target table
# MAGIC INSERT OVERWRITE l3_iga.Products
# MAGIC VALUES
# MAGIC    (1, 'Tea', null, '2021-12-31', '9999-12-31', 1, 0),
# MAGIC    (2, 'Tea', 20.00, '2021-01-01', '2021-12-31', 0, 0),
# MAGIC    (3, 'Tea', 10.00, '2019-01-01', '2021-01-01', 0, 0),
# MAGIC    (4, 'Coffee', 20.00, '2021-01-01', '9999-12-31', 1, 1),
# MAGIC    (5, 'Muffin', 30.00, '2020-03-01', '9999-12-31', 1, 1),
# MAGIC    (6, 'Biscuit', 40.00, '2022-01-24', '9999-12-31', 1, 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create source table
# MAGIC CREATE OR REPLACE TABLE l3_iga.UpdatedProducts
# MAGIC (
# MAGIC    ProductID INT,
# MAGIC    ProductName string,
# MAGIC    Rate float
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC --Insert records into source table
# MAGIC INSERT OVERWRITE l3_dospigamigration.UpdatedProducts
# MAGIC VALUES
# MAGIC    (1, 'Tea', null),
# MAGIC    (2, 'Coffee', 25.00),
# MAGIC    (3, 'Muffin', 35.00),
# MAGIC    (5, 'Pizza', 60.00)

# COMMAND ----------

def history_scd2(target_table: str, source_table: str, merge_condition: str, matched_condition: str, soft_delete=False, hard_delete=False):
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
    # These 2 queries must be run together for soft delete
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

def modify_matched_condition_scd2(target_table: str, source_table: str, matched_condition: str):
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

target_table='l3_dospigamigration.Products'
source_table='l3_dospigamigration.UpdatedProducts'
match_cond = modify_matched_condition_scd2(target_table, target_table, "TARGET.ProductName <> SOURCE.ProductName OR TARGET.Rate <> SOURCE.Rate")

historization_scd2(
    target_table,
    source_table,
    merge_condition='(TARGET.ProductID = SOURCE.ProductID)',
    matched_condition=match_cond,
    soft_delete=True
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from l3_dospigamigration.Products

# COMMAND ----------


