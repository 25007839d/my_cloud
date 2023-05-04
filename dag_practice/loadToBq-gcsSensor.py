import airflow
from _datetime import datetime,timedelta
from airflow import DAG
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.utils import trigger_rule
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor,GCSObjectUpdateSensor


default_dag_args = {
     'start_date': datetime(2022,10,20),
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1,
     'retry_delay' : timedelta(minutes=5),
    'catchup':False
}

output_file = 'gs://ritu_bucket/data/address1.csv'
#Replace <Your bucket> with your path details
with DAG(
       dag_id='demo_bq_dag',
       schedule_interval = timedelta(minutes = 5),
       default_args = default_dag_args) as dag:

      # bq_airflow_commits_query = BigQueryOperator(
      #    task_id = 'bq_airflow_commits_query',
      #    sql = """ SELECT *
      #    FROM [spatial-shore-360518:gcs_to_bq_df.emp]
      #     """)
         

      # export_commits_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
      #    task_id = 'export_airflow_commits_to_gcs',
      #    source_project_dataset_table = 'spatial-shore-360518:gcs_to_bq_df.emp',
      #    destination_cloud_storage_uris = [output_file],
      #    export_format = 'CSV')
#Checks for the existence of a file in Google Cloud Storage.
      # wait_for_file = GCSObjectExistenceSensor(
      #     task_id='wait_for_file',
      #     bucket='ritu_bucket',
      #     object='address1.csv',
      #     google_cloud_conn_id='google_cloud_storage_default'
      #                  )

# check updatede file in bucket or folder
      wait_for_updated_file = GCSObjectUpdateSensor(
          task_id='wait_for_file',
          bucket='ritu_bucket',
          object='data/address1.csv',
          google_cloud_conn_id='google_cloud_storage_default'
      )
      task = BashOperator(
         task_id = 'task_test',
         bash_command='date'
                         )


      wait_for_updated_file>>task











