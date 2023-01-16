# Databricks notebook source
# MAGIC %run "./utils"

# COMMAND ----------

from logger import Logger
import os

# COMMAND ----------

notebook_name = '2-TR ETL Relation'
env = os.environ['ENV']
l=Logger(sc,'IGA', notebook_name,'test_run', env)

# COMMAND ----------

l.info(f'Running notebook {notebook_name}...')

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("l2_db","l2_iga") 
dbutils.widgets.text("l3_db","l3_iga") 
dbutils.widgets.text("date","2022-10-14") 

l2_db = dbutils.widgets.get("l2_db")
l3_db = dbutils.widgets.get("l3_db")
date_to_process = dbutils.widgets.get("date")

print(l2_db, l3_db, date_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC # IGA_STG_REL_ENTITY_ENTITLEMENT

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {l3_db}.IGA_STG_REL_ENTITY_ENTITLEMENT (application, attribute, value, user)
SELECT application, attribute, value, user
FROM {l2_db}.igarolemodel
WHERE role IS NULL
AND to_date(file_date) = date_sub(DATE'{date_to_process}', 1)
AND InfoType = "EntitlementAdditional"
AND Attribute != "detectedRoles"
GROUP BY USER, APPLICATION, ATTRIBUTE, VALUE
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # IGA_STG_REL_ROLE_ENTITLEMENT

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {l3_db}.IGA_STG_REL_ROLE_ENTITLEMENT (ROLE, APPLICATION, ATTRIBUTE, VALUE)
SELECT ROLE, APPLICATION, ATTRIBUTE, VALUE
FROM {l2_db}.igarolemodel
WHERE infoType = "Entitlement" AND roleType = "it"
AND to_date(file_date) = date_sub(DATE'{date_to_process}', 1)
GROUP BY ROLE, APPLICATION, ATTRIBUTE, VALUE
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # IGA_STG_REL_IGA_ASSIGNED

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {l3_db}.IGA_STG_REL_IGA_ASSIGNED (ROLE, USER)
SELECT ROLE, USER
FROM {l2_db}.igarolemodel
WHERE InfoType = "Assigned" AND attribute <> "AllowedBy"
AND to_date(file_date) = date_sub(DATE'{date_to_process}', 1)
GROUP BY ROLE, USER
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # IGA_STG_REL_ROLE_PARENT

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {l3_db}.IGA_STG_REL_ROLE_PARENT (role, value, roletype)
SELECT role, value, roletype
FROM {l2_db}.igarolemodel
WHERE InfoType = "Parent"
AND to_date(file_date) = date_sub(DATE'{date_to_process}', 1)
AND RoleType  != "organizational"
GROUP BY ROLE, VALUE, ROLETYPE
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # IGA_STG_REL_ROLE_REQUIRED

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {l3_db}.IGA_STG_REL_ROLE_REQUIRED (role, value)
SELECT role, value
FROM {l2_db}.igarolemodel
WHERE InfoType = "Requirement"
AND to_date(file_date) = date_sub(DATE'{date_to_process}', 1)
GROUP BY ROLE, VALUE
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # IGA_STG_REL_ROLE_PERMITTED

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {l3_db}.IGA_STG_REL_ROLE_PERMITTED (role, value)
SELECT role, value
FROM {l2_db}.igarolemodel
WHERE InfoType = "Permit"
AND to_date(file_date) = date_sub(DATE'{date_to_process}', 1)
GROUP BY ROLE, VALUE
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # IGA_STG_REL_ROLE_OPTIONAL

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {l3_db}.IGA_STG_REL_ROLE_OPTIONAL (role, value, user)
SELECT role, value, user
FROM {l2_db}.igarolemodel
WHERE InfoType = "Assigned"
AND Attribute  = "AllowedBy"
AND to_date(file_date) = date_sub(DATE'{date_to_process}', 1)
GROUP BY ROLE, VALUE, USER
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # IGA_STG_REL_IDENTITY_ACCOUNT_ENTITLEMENT

# COMMAND ----------

q_fetch_direct_ent = spark.sql(f"""
    SELECT
      DIM_DOSIGA_C_ROLE_MODEL.N_USER USER,
      DIM_DOSIGA_C_ROLE_MODEL.N_ACCOUNT ACCOUNT,
      NULL,
      DIM_DOSIGA_C_ROLE_MODEL.N_APPLICATION APPLICATION,
      DIM_DOSIGA_C_ROLE_MODEL.T_ATTRIBUTE ATTRIBUTE,
      DIM_DOSIGA_C_ROLE_MODEL.T_VALUE VALUE
    FROM
      {l3_db}.iga_dim_c_role_model AS DIM_DOSIGA_C_ROLE_MODEL
    WHERE
      DIM_DOSIGA_C_ROLE_MODEL.T_SOURCE is null
""")

q_fetch_direct_ent = q_fetch_direct_ent.withColumnRenamed('NULL', 'ROLE')

# COMMAND ----------

q_fetch_base = spark.sql(f"""
  SELECT
    DISTINCT DIM_DOSIGA_C_ROLE_MODEL.S_ROLEMODEL_ID,
    DIM_DOSIGA_C_ROLE_MODEL.N_USER,
    DIM_DOSIGA_C_ROLE_MODEL.N_ACCOUNT,
    DIM_DOSIGA_C_ROLE_MODEL.N_ROLE,
    DIM_DOSIGA_C_ROLE_MODEL.N_APPLICATION,
    DIM_DOSIGA_C_ROLE_MODEL.T_ATTRIBUTE,
    DIM_DOSIGA_C_ROLE_MODEL.T_VALUE,
    DIM_DOSIGA_C_ROLE_MODEL.T_SOURCE
  FROM
    {l3_db}.iga_dim_c_role_model AS DIM_DOSIGA_C_ROLE_MODEL
  WHERE
    DIM_DOSIGA_C_ROLE_MODEL.C_INFOTYPE in (
      'IdentityEntitlement',
      'AssignedEntitlement',
      'EntitlementAdditional',
      'EntitlementException'
    )
    and DIM_DOSIGA_C_ROLE_MODEL.C_ROLETYPE is null
""")

# COMMAND ----------

q_get_itroles = spark.sql(f"""
  SELECT
    DISTINCT DIM_DOSIGA_C_ROLE_MODEL.N_ROLE
  FROM
    {l3_db}.iga_dim_c_role_model AS DIM_DOSIGA_C_ROLE_MODEL
  WHERE
    lower(DIM_DOSIGA_C_ROLE_MODEL.C_ROLETYPE) = 'it'
""")

# COMMAND ----------

q_fetch_sources = spark.sql(f"""
    SELECT
      DISTINCT DIM_DOSIGA_C_ROLE_MODEL.S_ROLEMODEL_ID,
      DIM_DOSIGA_C_ROLE_MODEL.T_SOURCE
    FROM
      {l3_db}.iga_dim_c_role_model AS DIM_DOSIGA_C_ROLE_MODEL
    WHERE
      DIM_DOSIGA_C_ROLE_MODEL.C_INFOTYPE in (
        'IdentityEntitlement',
        'AssignedEntitlement',
        'EntitlementAdditional',
        'EntitlementException'
      )
      and DIM_DOSIGA_C_ROLE_MODEL.C_ROLETYPE is null
      and DIM_DOSIGA_C_ROLE_MODEL.T_SOURCE is not null
""")

# COMMAND ----------

from pyspark.sql.functions import split, explode

udt_py_explode = q_fetch_sources.withColumn('T_SOURCE',explode(split('T_SOURCE',',')))
udt_py_explode.createOrReplaceTempView("udt_py_explode")

# COMMAND ----------

q_get_sources_in_rows = spark.sql(f"""
    SELECT
      cast(UDT_Py_Explode_Sources.S_ROLEMODEL_ID AS int),
      ltrim(T_SOURCE) AS N_ROLE
    FROM
      udt_py_explode AS UDT_Py_Explode_Sources
    WHERE
      UDT_Py_Explode_Sources.S_ROLEMODEL_ID is not null
""")

# COMMAND ----------

q_get_itroles.createOrReplaceTempView("q_get_itroles")
q_get_sources_in_rows.createOrReplaceTempView("q_get_sources_in_rows")

# COMMAND ----------

q_filter_sources = spark.sql(f"""
  SELECT
    DISTINCT q_get_sources_in_rows.S_ROLEMODEL_ID,
    q_get_sources_in_rows.N_ROLE
  FROM
    q_get_itroles INNER JOIN q_get_sources_in_rows ON (
      q_get_itroles.N_ROLE = q_get_sources_in_rows.N_ROLE
    )
""")

# COMMAND ----------

q_fetch_base.createOrReplaceTempView("q_fetch_base")
q_filter_sources.createOrReplaceTempView("q_filter_sources")

# COMMAND ----------

q_join = spark.sql(f"""
  SELECT
    DISTINCT q_fetch_base.N_USER USER,
    q_fetch_base.N_ACCOUNT ACCOUNT,
    q_filter_sources.N_ROLE ROLE,
    q_fetch_base.N_APPLICATION APPLICATION,
    q_fetch_base.T_ATTRIBUTE ATTRIBUTE,
    q_fetch_base.T_VALUE VALUE
  FROM
    q_fetch_base INNER JOIN q_filter_sources ON (
      q_fetch_base.S_ROLEMODEL_ID = q_filter_sources.S_ROLEMODEL_ID
    )
""")

# COMMAND ----------

q_fetch_direct_ent.createOrReplaceTempView("q_fetch_direct_ent")
q_join.createOrReplaceTempView("q_join")

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {l3_db}.IGA_STG_REL_IDENTITY_ACCOUNT_ENTITLEMENT
SELECT * FROM q_fetch_direct_ent
UNION ALL
SELECT * FROM q_join
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # IGA_STG_REL_IDENTITY_ACCOUNT

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {l3_db}.IGA_STG_REL_IDENTITY_ACCOUNT (APPLICATION, USER, ACCOUNT)
SELECT APPLICATION, USER, ACCOUNT
FROM {l2_db}.igarolemodel
WHERE InfoType IN ("IdentityEntitlement", "AssignedTarget")
AND to_date(file_date) = date_sub(DATE'{date_to_process}', 1)
GROUP BY APPLICATION, USER, ACCOUNT
""")

# COMMAND ----------

l.info(f'Finished notebook {notebook_name}')
