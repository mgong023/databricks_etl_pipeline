from datetime import timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago
from operators.snow.snow import CreateSnowIncidentOperator

GLOBAL_SNOW_CONFIG_FILE = '/opt/airflow/dags/operators/snow/servicenow_incident_global_variables.json'
polling_period = 1

default_args = {
    'owner': 'DOPRASIMBI',
    'depends_on_past': False,
    'start_date': days_ago(0),
}

with DAG(
    '<name>', 
    default_args=default_args, 
    schedule_interval='0 8 * * 1-5', 
    catchup=False, 
    is_paused_upon_creation=False
) as dag:
    task_0 = DatabricksSubmitRunOperator(
        task_id = 'orchestrator',
        databricks_conn_id = 'databricks_default',
        polling_period_seconds = polling_period,
        run_name = 'orchestrator',
        notebook_task = {
            'notebook_path': '/Repos/Projects/<repo_name>/notebooks/orchestrator'
        },
        existing_cluster_id = '0221-085548-cd3o72ck',
        do_xcom_push=True       
    )

    create_incident_task = CreateSnowIncidentOperator(
        task_id='create_incident',
        snow_params_path=GLOBAL_SNOW_CONFIG_FILE,
        incident_description='L2 checks failed or ETL failed',
        application_identifier='<name> ETL or checks failed',
        trigger_rule='one_failed',
        databricks_conn_id='databricks_default',
        databricks_run_task_id='orchestrator'
    )
    
    task_0 >>  create_incident_task