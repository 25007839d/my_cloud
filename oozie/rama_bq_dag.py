
import os
import yaml
import sys
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator,DataprocSubmitHiveJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
#FILE CREATION BY CLIENT LIB
from google.cloud import storage
def get_file():

    storage_client = storage.Client()
    bucket = storage_client.get_bucket("demo_011")

    blob = bucket.blob("bq.sql")

    sql =blob.download_as_text()
    return sql
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,9,14),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,


}
dag = DAG(
    dag_id="antenna_bqscript_load",
    schedule_interval='0 9 * * *',
    catchup=False,
    default_args=default_args,
    concurrency=3,
    tags= ["dtwin", "RK"]
)


select_query_job_dag_location = BigQueryInsertJobOperator(
    task_id="select_query_job_dag_location",
    gcp_conn_id="google_cloud_default",
    configuration={
        "query": {
            "query": "{% include 'bq.sql' %}",

            "useLegacySql": False,
        }
    },
    dag=dag
)
select_query_job_other_gcs_location = BigQueryInsertJobOperator(
    task_id="select_query_job_other_gcs_location",
    gcp_conn_id="google_cloud_default",
    configuration={
        "query": {
            "query": get_file(),

            "useLegacySql": False,
        }
    },
    dag=dag
)