from airflow import DAG,settings
from _datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_delete_operator import GCSDeleteObjectsOperator
from google.cloud import storage

from pathlib import Path
import os,sys, stat

bucket_n = "demo_0111"

current_time = datetime.now()
current_time1 = current_time.strftime("%d-%m-%Y")

file = open('_SUCCESS.txt', 'a+')


# a=file.close()


# FILE CREATE BY OS
def create_file():


    current_time = datetime.now()
    current_time1 = current_time.strftime("%d-%m-%Y")

    path = "gs://demo_0111" + "/" + current_time1 + "/" + "dataset" + "/" + "_SUCCESS" + "/"
    print(path)

    os.system("touch rama.txt")

    current_dir = os.getcwd()
    current_dir_path = current_dir + "rama.txt"

    c = f"""gsutil mv {current_dir_path} {path}"""
    os.system(c)

#FILE CREATION BY CLIENT LIB
def create_file_lib():
    current_time = datetime.now()
    current_time1 = current_time.strftime("%d-%m-%Y")

    file = open('_SUCCESS.txt', 'a+')
    # a=file.close()

    storage_client = storage.Client()
    bucket = storage_client.get_bucket('demo_011')

    path = f"""/dtwin/vzw/internal/ran_hive_ddl_success/{current_time1}/vzw.ran.dim.inventory.enodeb_site_map.norm.v0"""
    filename = "%s/%s" % (current_time1 + '/dataset', '_SUCCESS.txt')

    nd ="gs://gudv-dev-dtwndo-0-usmr-warehouse/dtwin/vzw/internal/ran_hive_ddl_success/${YEAR}${MONTH}${DAY}/vzw.ran.dim.inventory.cell_sites.norm.v0"


    blob = bucket.blob(filename)

    blob.upload_from_file(file)

default_arg = {
                'owner':"oozie",
                'start_date':datetime(2022,1,18),
                'retry': 3,
                'retry_delay':timedelta(minutes=1),
                'catchup':False,
                'schedule_interval':"*/20 * * * *"


                }
with DAG(dag_id="oozie_dag",default_args=default_arg, tags=[current_time1]) as dag:
    command = """ 
        export GOOGLE_APPLICATION_CREDENTIALS=/home/airflow/gcs/dags/dushyant-373205-411c3f8b3abc.json 
         gcloud auth activate-service-account --key-file=/home/airflow/gcs/dags/dushyant-373205-411c3f8b3abc.json 
         """

    ingest_and_process = BashOperator(
                        task_id= "ingest_and_process",
                        bash_command= command
    )

    create_f = PythonOperator(
                        task_id='create_empty_file_OS',
                        python_callable=create_file,
                        dag=dag,

    )

            # OR

    create_f2 = PythonOperator(
        task_id='create_empty_file_CLIENT_LIB',
        python_callable=create_file_lib,
        dag=dag,

    )