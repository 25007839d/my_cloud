"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import datetime
import airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'start_date': datetime.datetime(2022,8,2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'airflow_monitoring',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20))

t1 = BashOperator(
    task_id='date1',
    bash_command='date',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31-1)
