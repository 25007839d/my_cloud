import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule



yesterday=datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args={
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': 'spatial-shore-360518'
}

with models.DAG(
        'airflow_wordcount',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    create_dataproc_cluster=dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='composer-hadoop-tutorial-cluster',
        num_workers=2,
        region='asia-east1',
        master_machine_type='n1-standard-2')



    delete_dataproc_cluster=dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='composer-hadoop-tutorial-cluster',
        region='asia-east1',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    create_dataproc_cluster >>  delete_dataproc_cluster

# create cluster by cli
#  gcloud dataproc clusters create dushyant    --region us-central1 --zone us-central1-b     --master-machine-type n1-standard-2     --master-boot-disk-size 50     --num-workers 2     --worker-machine-type n1-standard-2     --worker-boot-disk-size 50     --project spatial-shore-360518