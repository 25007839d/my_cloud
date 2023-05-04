from airflow import DAG,settings
from _datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_delete_operator import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

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



#FILE CREATION BY CLIENT LIB
def create_file_lib(**kwargs):


    file = open('_SUCCESS', 'a+')


    storage_client = storage.Client()
    bucket = storage_client.get_bucket(kwargs['bucket'])

    filename = "%s/%s" % (kwargs['path'], kwargs['file_name'])

    blob = bucket.blob(filename)

    blob.upload_from_file(file)


default_arg = {
                'owner':"oozie",
                'start_date':datetime(2022,1,18),
                'retry': 3,
                'retry_delay':timedelta(minutes=1),
                'catchup':True,
                'schedule_interval':"*/20 * * * *"


                }
with DAG(dag_id="oozie_dag2",default_args=default_arg,tags= [current_time1]) as dag:



    wait_for_file_1 = GCSObjectExistenceSensor(
        task_id='wait_for_file_1',
        bucket='demo_011',
        object=check_path_1,
        google_cloud_conn_id='google_cloud_storage_default',
        delegate_to = None,
        impersonation_chain = None,
        retry= timedelta(minutes=1)
    )

    wait_for_file_2 = GCSObjectExistenceSensor(
        task_id='wait_for_file_2',
        bucket='demo_011',
        object=check_path_2,
        google_cloud_conn_id='google_cloud_storage_default'
    )



    create_f1 = PythonOperator(
        task_id='create_flag_1',
        python_callable=create_file_lib,
        dag=dag,
        op_kwargs={'path':path_1,'bucket':bucket_n,'file_name':'_SUCCESS'}

    )
    create_f2 = PythonOperator(
        task_id='create_flag_2',
        python_callable=create_file_lib,
        dag=dag,
        op_kwargs={'path': path_2, 'bucket': bucket_n, 'file_name': '_SUCCESS'}

    )

    end = DummyOperator(
                        task_id= "end"
    )


    wait_for_file_1,wait_for_file_2,create_f1,create_f2>>end
