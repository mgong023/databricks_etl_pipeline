# Databricks notebook source
# MAGIC %md
# MAGIC # Preparing tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TARGET
# MAGIC CREATE OR REPLACE TABLE l3_iga.Products_v2
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
# MAGIC INSERT OVERWRITE l3_iga.Products_v2
# MAGIC VALUES
# MAGIC    (1, 'Tea', 10.00, '2021-12-31', null),
# MAGIC    (2, 'Coffee', 22.00, '2022-10-15', null),
# MAGIC    (3, 'Coffee', 20.00, '2021-01-01', '2022-10-14'),
# MAGIC    (4, 'Biscuit', 40.00, '2023-01-24', null),
# MAGIC    (5, 'Pizza', 10.00, '2023-02-08', null)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create source table
# MAGIC CREATE OR REPLACE TABLE l3_iga.UpdatedProducts_v2
# MAGIC (
# MAGIC    ProductName string,
# MAGIC    Rate float
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC --Insert records into source table
# MAGIC INSERT OVERWRITE l3_iga.UpdatedProducts_v2
# MAGIC VALUES
# MAGIC    ('Tea', 12.00), --updated
# MAGIC    ('Coffee', 25.00), --updated
# MAGIC    ('Muffin', 35.00), --new
# MAGIC    ('Pizza', 10.00) --unchanged
# MAGIC    --biscuit is deleted

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (SELECT
# MAGIC   -1 AS id,
# MAGIC   *,
# MAGIC   current_date AS VERSION_START,
# MAGIC   null AS VERSION_END
# MAGIC FROM
# MAGIC   l3_iga.UpdatedProducts_v2
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   l3_iga.Products_v2 TARGET
# MAGIC WHERE
# MAGIC   EXISTS (
# MAGIC     SELECT
# MAGIC       *
# MAGIC     FROM
# MAGIC       l3_iga.UpdatedProducts_v2 SOURCE
# MAGIC     WHERE
# MAGIC       TARGET.Productname = SOURCE.Productname
# MAGIC   )
# MAGIC   AND VERSION_END is null)

# COMMAND ----------

def scd2_simple(target_table: str, source_table: str, key_columns: list):
    """
    Expects incremental data changes.
    
    Returns: nothing
    """
    merge_condition = ''.join(f'TARGET.{key} = SOURCE.{key} AND ' for key in key_columns)[:-5]
    spark.sql(f"""
        MERGE INTO {target_table} TARGET
        USING 
        (
          SELECT -1 AS id, *, current_date AS VERSION_START, null AS VERSION_END
          FROM {source_table}
          UNION ALL
          SELECT *
          FROM {target_table} TARGET
          WHERE EXISTS
          (
            SELECT * FROM {source_table} SOURCE
            WHERE {merge_condition}
          )
          AND VERSION_END is null
        ) AS SOURCE
        ON TARGET.id = SOURCE.id
        WHEN MATCHED and TARGET.VERSION_END is null THEN
        UPDATE SET TARGET.VERSION_END = current_date()
        WHEN NOT MATCHED THEN 
        INSERT *
    """)
    spark.sql(f"""
        UPDATE {target_table} TARGET SET VERSION_END = '1000-12-31'
        WHERE NOT EXISTS(SELECT * FROM {source_table} SOURCE WHERE {merge_condition})
    """)
    print(f'Finished {target_table} historization')

# COMMAND ----------

scd2_simple('l3_iga.Products_v2', 'l3_iga.UpdatedProducts_v2', ['ProductName'])

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from l3_iga.Products_v2

# COMMAND ----------


