from datetime import timedelta
import os
import yaml
import sys
from datetime import datetime,time
from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageUploadSessionCompleteSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, \
    DataprocSubmitHiveJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.dummy import DummyOperator
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from typing import TYPE_CHECKING, Callable, Sequence

from google.api_core.retry import Retry
from google.cloud.storage.retry import DEFAULT_RETRY




# def get_dag_state(execution_date, **kwargs):

#     from time import sleep
#     sleep(10)
#     previous_day_time = execution_date - timedelta(minutes=1)
#
#     # current_time1 = current_time.strftime("%Y-%m-%d")
#
#
#     ti = get_task_instance('DAG_ID', 'wait_for_file_1', execution_date)
#     task_status = ti.current_state()
#
#
#     if task_status=="success":
#         return "start2"
#     elif task_status!="success":
#         raise Exception("Sorry, no numbers below zero")
#
#     elif task_status != "success":
#         return "end"
def get_dag_state(**kwargs):
    from google.cloud import storage
    client = storage.Client()

    bucket = client.get_bucket(kwargs['bucket_name'])

    blobs = bucket.list_blobs()

    for blob in blobs:
        if kwargs['prefix'] == blob.name:
            print(blob.name)
            return "run_spark_antenna"

        for i in range(1,5):
            second =  i*60
            from time import sleep
            sleep(second)
            blobs = bucket.list_blobs()
            for blob in blobs:
                if kwargs['prefix'] == blob.name:
                    print(blob.name)
                    return "start2"

        else:

            return 'end'



# def get_dag_state(execution_date, **kwargs):
#     from time import sleep
#     sleep(60)
#     ti = get_task_instance('DAG_ID', 'wait_for_file_1', execution_date)
#     task_status = ti.current_state()
#     print(task_status,"==================")
#     if task_status=="success":
#         return "start2"
#     else:
#         return "end1"



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,2,7),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(
    dag_id='DAG_ID',
    schedule_interval="* */9 * * *",
    catchup=False,
    default_args=default_args,
    concurrency=3,
    tags= ["dtwin", "SM", "RK"]
)



start = DummyOperator(task_id='start', dag=dag)



# dag_status = BranchPythonOperator(
#                 task_id='dag_status',
#                 python_callable= get_dag_state,
#                 dag=dag)
dag_status = BranchPythonOperator(
    task_id='dag_status',
    python_callable=get_dag_state,
    dag=dag,
    op_kwargs={'bucket_name':"demo_011",'prefix':"dtwin/vzw/internal/ran_hive_ddl_success/27-01-2023/vzw.ran.dim.inventory.cell_sites.norm.v0/_SUCCES.txt"}
    )

end = DummyOperator(task_id='end', dag=dag)
end1 = DummyOperator(task_id='end1', dag=dag)
start2 = DummyOperator(task_id='start2', dag=dag)
start>>dag_status>>end
start>>dag_status>>start2>>end1




