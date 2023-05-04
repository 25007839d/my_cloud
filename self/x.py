from datetime import timedelta
import os
import yaml
import sys
from datetime import datetime,time
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

def get_dag_state(**kwargs):
    from google.cloud import storage
    client = storage.Client()

    bucket = client.get_bucket(kwargs['bucket_name'])

    blobs = bucket.list_blobs()

    for blob in blobs:
        if kwargs['prefix'] == blob.name:
            print(blob.name)
            return "start2"

        else:
            return 'end'



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
    schedule_interval="47 13 * * 1",
    catchup=False,
    default_args=default_args,
    concurrency=3,
    tags= ["dtwin", "SM", "RK"]
)



start = DummyOperator(task_id='start', dag=dag)




# dag_status = BranchPythonOperator(
#     task_id='dag_status',
#     python_callable=get_dag_state,
#     dag=dag,
#     op_kwargs={'bucket_name':"demo_011",'prefix':"dtwin/vzw/internal/ran_hive_ddl_success/27-01-2023/vzw.ran.dim.inventory.cell_sites.norm.v0/_SUCCESS"}
#     )
#
# end = DummyOperator(task_id='end', dag=dag)
#
# end1 = DummyOperator(task_id='end1', dag=dag)
#
# start2 = DummyOperator(task_id='start2', dag=dag)

# start>>dag_status>>start2>>end1




