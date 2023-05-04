import datetime

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataprocClusterDeleteOperator, DataProcPySparkOperator


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

PYSPARK_JOB = {"reference":{"project_id":"spatial-shore-360518"},
              "placement": {"cluster_name":"cluster-9ab1"},
               "pyspark_job": {"main_py_file_uri": f"gs://ritu_bucket/py.py"}
    }
default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': datetime.datetime(2022, 10, 25),
    'catchup': False
}

with models.DAG(
        'composer_call_bashoperator_python',
        schedule_interval=datetime.timedelta(minutes=5),
        default_args=default_dag_args) as dag:

    submit_pyspark = DataProcPySparkOperator(
        task_id="run_pyspark_etl",
        main=f"gs://ritu_bucket/py.py",
        cluster_name="cluster-9ab1",
        region="us-central1"
    )