"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import datetime
import airflow 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator

from airflow.contrib.operators import dataproc_operator

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}

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

dag2 = DAG('rahul'
    ,default_args=default_args,
    description='hourly schedule',
    schedule_interval=timedelta(hours=8),
    dagrun_timeout=timedelta(minutes=10),
    catchup=False
    )

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id='date1',
    bash_command='date',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31-1)
# t2 = BashOperator(
#     task_id='decorator',
#     bash_command='python /home/airflow/gcs/dags/deco.py',
#     dag=dag,
#     depends_on_past=False)
# t3 = BashOperator(
#     task_id='datproc',
#     bash_command='gs://pcm_dev-ingestion1/apache.py',
#     dag=dag,
#     depends_on_past=False)
# t4 = BashOperator(
#     task_id='apache-beam',
#     bash_command='python /home/airflow/gcs/dags/apache_df.py',
#     dag=dag,
#     depends_on_past=False)
# t5 = BashOperator(
#     task_id='datproc_cluster_create',
#     bash_command='python /home/airflow/gcs/dags/create_cluster.py',
#     dag=dag,
#     depends_on_past=False)
# # t1>>t2>>t3 >>t4

create_cluster = dataproc_operator.DataprocCreateClusterOperator(
        task_id="dush-dataproc",
        project_id='inlaid-tribute-353619',
        cluster_config=CLUSTER_CONFIG,
        region='us-central1',
        cluster_name='dushdata',
        dag=dag,
        depends_on_past=False)

t1>>create_cluster
print("cluster create")