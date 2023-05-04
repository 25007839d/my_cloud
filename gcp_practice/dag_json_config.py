from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import json
import google.auth
from google.cloud import storage
credintial, project = google.auth.default()
import os
os.environ["GCLOUD_PROJECT"]="ritu_bucket"

# read json_confing by cloud_storage_client
# def get_json_gcs():
# create storage clint
bucket = 'ritu_bucket'
file_name = 'dag_arg.json'
clint = storage.Client(credentials=credintial)

    # get bucket name
bucket = clint.get_bucket(bucket)

    # get blob
blob = bucket.get_blob(file_name)
print(blob)
file = json.loads(blob.download_as_string())

print(file)


for i in file:
    print(i)
schema=StructType([])

    data_types={
        "integer":IntegerType(),
        "string":StringType(),
        "date":DateType(),
        "integer":IntegerType()

for i in schema_rdd:
    schema.add(i.get("name"), data_types.get(i.get('type')))

# bucket = 'us-central1-fjdk-274d9023-bucket'
# file_name = 'data/dag_arg.json'
# json_file = get_json_gcs()
# print(json_file)


# with open(r'gs://us-central1-fjdk-274d9023-bucket/data/dag_arg.json','r')as file:
#     file_json = json.load(file)



# def task1_function():
#     print("task success")
# default_args = {
#                 'owner': 'airflow',
#                 'start_date': datetime(2022, 10 ,20),
#                 'retrys':2,
#                 'retry_delay': timedelta(minutes=2),
#                 'depends_on_past':False,
#                 'email_on_retry': False,
#                 'email_on_failure': False,
#                 'project_id':'united-monument-360105',
#                 'schedule_interval':"*/20 * * * *"
#                 }

# with DAG(dag_id='sub_dag_operator',
#          default_args=default_args
#          ):
#     task1 = PythonOperator(
#         task_id = 'dummy',
#         python_callable=task1_function
#      )

