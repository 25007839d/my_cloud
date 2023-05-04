import datetime

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataprocClusterDeleteOperator, DataProcPySparkOperator


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())


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

    # run_python = bash_operator.BashOperator(
    #     task_id='run_python3',
    #     # This example runs a Python script from the data folder to prevent
    #     # Airflow from attempting to parse the script as a DAG.
    #     bash_command='python2 /home/airflow/gcs/data/check_path_glob.py')
    t1 = DummyOperator(
        task_id='t1'
                )

    t2 = DummyOperator(
        task_id='t2'
         )

    t3 = DummyOperator(
        task_id='t3'
                )

    t1 >>t3