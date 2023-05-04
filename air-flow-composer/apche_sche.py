import datetime
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator

from airflow.contrib.operators import dataproc_operator
from airflow.providers.apache.beam.operators.beam import (
    BeamRunJavaPipelineOperator,
    BeamRunPythonPipelineOperator,
)



default_args = {
                'start_date': datetime.datetime(2022,8,2),
                'retries': 2,
                'retry_delay': timedelta(minutes=10)
            }

dag = DAG(
    'dushyant',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval=timedelta(minutes=100),
    dagrun_timeout=timedelta(minutes=20),
    catchup=False
    )



start_python_job_async = BeamRunPythonPipelineOperator(
    task_id="start-python-job-async",
    runner="DataflowRunner",
    py_file='python /home/airflow/us-central1-pipeline-a83589e9-bucket/dags/print.py',
    dag=dag,


    py_requirements=['apache-beam[gcp]==2.25.0'],
    py_interpreter='python3',
    py_system_site_packages=False,
    dataflow_config={
        "job_name": "start-python-job-async",
        "location": 'us-central1-a',
        "wait_until_finished": False,
    },

)

