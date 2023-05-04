zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzcnnbnbmnbvbvvbvbvbbvbvbvbbvvbbbvbbvbvbvbb        bbbgg                 hbb                                                                                                        gfrom airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
import json
import google.auth
from google.cloud import storage
credintial, project = google.auth.default()

# read json_confing by cloud_storage_client
def get_json_gcs(bucket, file_name):
    # create storage clint
    clint = storage.Client(credentials=credintial)

    # get bucket name
    bucket = clint.get_bucket(bucket)

    # get blob
    blob = bucket.get_blob(file_name)

    # load blob using json
    # file_data = json.load(blob.download_as_string(client=None))
    file_data = json.loads(blob.download_as_text(client=None))
    for i in file_data:
        return print(i)


bucket = 'us-central1-fjdk-274d9023-bucket'
file_name = 'data/dag_arg.json'
json_file = get_json_gcs(bucket,file_name)
print(json_file)



# read json file from airflow bucket

with open(r'/home/airflow/gcs/data/dag_arg.json','r')as file:
    file_json = json.load(file)


def task1_function():

    print('json read ')

default_args = {
                'owner': 'airflow',
                'start_date': eval(file_json['start_date']),
                'retrys':2,
                'retry_delay': timedelta(minutes=2),
                'depends_on_past':False,
                'email_on_retry': False,
                'email_on_failure': False,
                'project_id':file_json['project_id'],
                'schedule_interval':"*/20 * * * *"
                }

with DAG(dag_id=file_json['dag_id'],
         default_args=default_args
         ) as dag:
    task1 = PythonOperator(
        task_id = 'dummy',
        python_callable=task1_function
     )
# sub DAG operator do with taskgroup
    with TaskGroup('group-1') as task_group:
        task3 = BashOperator(
            task_id='task_3',
            bash_command='sleep 3'
        )

        task4 = BashOperator(
            task_id='task_4',
            bash_command='sleep 10'
        )
        task3>>task4
    task1>>task_group