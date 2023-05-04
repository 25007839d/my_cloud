"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import datetime
import airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'start_date': datetime.datetime(2022,8,2),
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}
'''
schedule_time: tells airflow when to trigger this DAG. In this case, the given DAG will executer after every hour. We can also define this via CRON expression.
catchup: If True, Airflow starts the DAG from given start_date (defined in default_args – 5 days ago). Else DAG starts on next execution date.
default_args: We define minimal default configurations that covers basics owner and start_date. These args will be passed to each operator, but can also be overwritten by each operator.

'''
dag = DAG(
    'dushyant',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval="* */9 * * *",
    dagrun_timeout=timedelta(minutes=20),
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
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 0,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},

    },
}

# [END how_to_cloud_dataproc_create_cluster]


# TIMEOUT = {"seconds": 1 * 24 * 60 * 60}


# [START how_to_cloud_dataproc_hive_config]


# [END how_to_cloud_dataproc_hive_config]



    # [START how_to_cloud_dataproc_create_cluster_operator]



create_cluster = DataprocCreateClusterOperator(
        task_id="dush-dataproc",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,

    )
    # [END how_to_cloud_dataproc_create_cluster_operator]

    # [START how_to_cloud_dataproc_delete_cluster_operator]
delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )
    # [END how_to_cloud_dataproc_delete_cluster_operator]
delete_cluster.trigger_rule = TriggerRule.ALL_DONE

create_cluster >> delete_cluster
