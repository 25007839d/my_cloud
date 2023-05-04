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

BASE_DIR = Path(__file__).resolve().parent.parent
dag_folder_path = os.path.dirname(settings.DAGS_FOLDER)
conf_file_path = os.path.join(dag_folder_path, 'dags')
current_dir = os.getcwd()


current_time = datetime.now()
previous_day_time = current_time-timedelta(days=1)

current_time1 = current_time.strftime("%d-%m-%Y")
previous_day = previous_day_time.strftime("%d-%m-%Y")
path_1 = f"""dtwin/vzw/internal/ran_hive_ddl_success/{current_time1}/vzw.ran.dim.inventory.enodeb_site_map.norm.v0"""
path_2 = f"""dtwin/vzw/internal/ran_hive_ddl_success/{current_time1}/vzw.ran.dim.inventory.cell_sites.norm.v0"""

check_path_1 = f"""dtwin/vzw/internal/ran_hive_ddl_success/{previous_day}/vzw.ran.dim.inventory.enodeb_site_map.norm.v0"""
check_path_2 = f"""dtwin/vzw/internal/ran_hive_ddl_success/{previous_day}/vzw.ran.dim.inventory.cell_sites.norm.v0"""
check_path_3 = f"""dtwi/vzw/internal/ran_hive_ddl_success/{current_time1}/vzw.ran.dim.inventory.cell_sites.norm.v0"""
# 2nd method to delete object by client lib
def delete_bucket(bucket_name):
    """Deletes a bucket. The bucket must be empty."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete()

    print(f"Bucket {bucket.name} deleted")



def chmod():
    os.chmod(current_dir + '/demo.txt', stat.S_IXGRP)
    print("Set a file execute by the group")

#FILE CREATION BY CLIENT LIB
def create_file_lib(**kwargs):


    file = open('_SUCCESS.txt', 'a+')
    # a=file.close()

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(kwargs['bucket'])

    filename = "%s/%s" % (kwargs['path'], kwargs['file_name'])

    blob = bucket.blob(filename)

    blob.upload_from_file(file)

def create_file():


    os.system("touch vzw.ran.dim.inventory.cell_sites.norm.v0")

    current_dir = os.getcwd()
    current_dir_path = current_dir + "touch vzw.ran.dim.inventory.cell_sites.norm.v0"

    c = f"""gsutil mv {current_dir_path} gs://demo_011/dtwi/vzw/internal/ran_hive_ddl_success/{current_time1}/"""

    os.system(c)


default_arg = {
                'owner':"oozie",
                'start_date':datetime(2022,1,18),
                'retry': 3,
                'retry_delay':timedelta(minutes=1),
                'catchup':True,
                'schedule_interval':"*/20 * * * *"


                }
with DAG(dag_id="oozie_dag2",default_args=default_arg,tags= [current_time1]) as dag:



    # wait_for_file_1 = GCSObjectExistenceSensor(
    #     task_id='wait_for_file_1',
    #     bucket='demo_011',
    #     object=check_path_1,
    #     google_cloud_conn_id='google_cloud_storage_default'
    # )

    # wait_for_file_2 = GCSObjectExistenceSensor(
    #     task_id='wait_for_file_2',
    #     bucket='demo_011',
    #     object=check_path_2,
    #     google_cloud_conn_id='google_cloud_storage_default'
    # )
    wait_for_file_3 = GCSObjectExistenceSensor(
        task_id='wait_for_file_3',
        bucket='demo_011',
        object=check_path_3,
        google_cloud_conn_id='google_cloud_storage_default'
    )




    # delete = BashOperator(
    #                     task_id="delete_bucket_folder",
    #                     bash_command="gsutil rm -r gs://demo_03111/subdir"
    #                     )
    #
    #
    # create_b = BashOperator(
    #                     task_id="create_bucket",
    #                     bash_command="gsutil mb gs://demo_011"
    #
    # )

    # create_f = PythonOperator(
    #                     task_id='create_empty_file',
    #                     python_callable=create_file,
    #                     dag=dag
    # )
    # change_m = PythonOperator(
                    #     task_id='give_file_permission',
                    #     python_callable=chmod,
                    #     dag=dag)

    # change_m = BashOperator(
    #                     task_id='give_file_permission',
    #                     bash_command="chmod 640 /home/airflow/demo.txt ")
    #
    # file_local_to_gcs = BashOperator(
    #                     task_id="file_cp_to_gcs",
    #                     bash_command="gsutil cp /home/airflow/demo.txt gs://demo_02111/script/"
    # )
    # create_f = PythonOperator(
    #     task_id='create_empty_file_OS',
    #     python_callable=create_file,
    #     dag=dag,
    #
    # )

    # OR

    # create_f1 = PythonOperator(
    #     task_id='create_flag_1',
    #     python_callable=create_file_lib,
    #     dag=dag,
    #     op_kwargs={'path':path_1,'bucket':bucket_n,'file_name':'_SUCCESS.txt'}
    #
    # )
    # create_f2 = PythonOperator(
    #     task_id='create_flag_2',
    #     python_callable=create_file_lib,
    #     dag=dag,
    #     op_kwargs={'path': path_2, 'bucket': bucket_n, 'file_name': '_SUCCESS.txt'}
    #
    # )
    create_f3 = PythonOperator(
        task_id='create_flag_3',
        python_callable=create_file,
        dag=dag,


    )

    end = DummyOperator(
                        task_id= "end"
    )


    wait_for_file_3,create_f3>>end
