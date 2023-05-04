from datetime import datetime, timedelta
from airflow import models, DAG, settings
# from airflow.sensors.external_task import ExternalTaskSensor
# from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.providers.google.cloud.transfer.gcs_to_gcs import GCSToGCSOperator
# from airflow.providers.google.cloud.transfer.local_to_gcs import LocalFilesystemToGCSOperator
import os
import json
import time
import google.auth
from google.cloud import storage
import json

# input_json = models.Variable.get('json_file_path')
# credintial, project = google.auth.default()
# clint = storage.Client(credentials=credintial)
# bucket = clint.list_buckets()
# for i in bucket:
#     print(i)


def get_json_gcs(bucket, file_name):
    # create storage clint
    clint = storage.Client()

    # get bucket name
    bucket = clint.get_bucket(bucket)

    # get blob
    blob = bucket.get_blob(file_name)

    # load blob using json
    file_data = json.load(blob.download_as_string(client=None))

    for i in file_data:
        return print(i)


bucket = 'us-central1-sajk-443b4a6f-bucket'
file_name = 'input/ritu-351906-27e10a6678af.json'
json_file = get_json_gcs(bucket,file_name)
print(json_file)

dag_folder_path = os.path.dirname(settings.DAGS_FOLDER)
conf_file_path = os.path.join(dag_folder_path, 'dags')


def dag_path():
    print("PPPPPPPPPPPPPPPPPPPPPPPPPP", conf_file_path, "uuuuuuuuuuuuuuuuuuuuuuuuuu")


def con_file():
    # with open(input_json,'r') as file:
    #     con = json.load(file)
    print('input_json')
def xcom_push(ti):
    a='5665'
    ti.xcom_push(key='xcom_push',value=a)
    print('xcom_push successfull')

def xcom_pull(**element):
    xcom_pull = element['ti'].xcom_pull(key='xcom_push')
    print('xcom_pull',xcom_pull)
default_args = {
    "owner": "gcs_telus",
    "start_date": datetime(2022, 10, 15),
    "retrys": 2,
    "retry_delay": timedelta(minutes=2)
}
with DAG(dag_id="gcp", schedule_interval='*/15 * * * *', default_args=default_args, catchup=False) as dag:
    task1 = PythonOperator(
        task_id='dag_path',
        python_callable=dag_path,
        dag=dag
    )

    task2 = PythonOperator(
        task_id='conf_file',
        python_callable=con_file,
        dag=dag
    )
    task3 = PythonOperator(
            task_id='xcom_push',
            python_callable= xcom_push
    )
# for pass fun arg with key value pair
    # task31 = PythonOperator(
    #     task_id='xcom_push',
    #     python_callable=xcom_push,
    #     op_kwargs={'list':'23,54,65'})

#  for pass arg dynamically use variable
#     task33 = PythonOperator(
#         task_id='xcom_push',
#         python_callable=xcom_push,
#         op_kwargs={'list':'{{var.value.list}}'
#                    }
#                     )

    task4 = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_pull

    )
    task2 >> task1>>task3>>task4
