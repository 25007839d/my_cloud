from xml.etree.ElementInclude import include

from airflow import DAG,settings
from _datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_delete_operator import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from google.cloud import storage

from pathlib import Path
import os,sys, stat

bucket_n = "demo_011"



current_time = datetime.now()
previous_day_time = current_time-timedelta(days=1)

current_time1 = current_time.strftime("%Y-%m-%d")
previous_day = previous_day_time.strftime("%d-%m-%Y")
path_1 = f"""dtwin/vzw/internal/ran_hive_ddl_success/{current_time1}/vzw.ran.dim.inventory.enodeb_site_map.norm.v0"""
path_2 = f"""dtwin/vzw/internal/ran_hive_ddl_success/{current_time1}/vzw.ran.dim.inventory.cell_sites.norm.v0"""

check_path_1 = f"""dtwin/vzw/internal/ran_hive_ddl_success/{previous_day}/vzw.ran.dim.inventory.enodeb_site_map.norm.v0"""
check_path_2 = f"""dtwin/vzw/internal/ran_hive_ddl_success/{previous_day}/vzw.ran.dim.inventory.cell_sites.norm.v0"""




default_arg = {
                'owner':"oozie",
                'start_date':datetime(2022,2,22),
                'retry': 3,
                'retry_delay':timedelta(minutes=1),
                'catchup':True,
                'schedule_interval':"*/20 * * * *"


                }
with DAG(dag_id="oozie_dag2",default_args=default_arg,tags= [current_time1]) as dag:

    start = DummyOperator(
        task_id= "start"
    )

    # task_explicit =BigQueryInsertJobOperator(
    #     task_id='bigquery_insert_job',
    #     gcp_conn_id='google_cloud_default',
    #     configuration={
    #         "query": "{% include 'bq.sql' %}",
    #             "useLegacySql": False
    #         }
    #
    # )
    t1 = BigQueryInsertJobOperator(
        task_id='bq_write_to_umc_cg_service_agg_stg',
        configuration={
            "query": "{% include 'bq.sql' %}",
            "useLegacySql": False,
            "allow_large_results": True,
            "writeDisposition": "WRITE_TRUNCATE",
            "destinationTable": {
                'projectId': 'dushyant-373205',
                'datasetId': 'gopal',
                'tableId': 'employee_name'
            }
        },
        params={'BQ_PROJECT': 'dushyant-373205'},
        gcp_conn_id='google_cloud_default',
        location='US',

    )