from datetime import datetime, timedelta, date

from airflow import models, DAG

from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataprocClusterDeleteOperator, DataProcPySparkOperator

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.operators.bash import BashOperator

from airflow.models import Variable

from airflow.utils.trigger_rule import TriggerRule

from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

import json

current_date = str(date.today())

BUCKET = "gs://bigquery_peter"

PYSPARK_JOB = BUCKET + "/job/Flights.py"

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=90),
    'project_id': 'united-monument-360105',
    "schedule_interval": "20 7 * * *",
}


with DAG("Flights_delay_etl", default_args=DEFAULT_DAG_ARGS) as dag:
    wait_for_file = GCSObjectExistenceSensor(
        task_id='wait_for_file',
        bucket='bigquery_peter',
        object=current_date + '.csv',
        google_cloud_conn_id='google_cloud_storage_default'
                                            )

    create_cluster = DataprocClusterCreateOperator(
        task_id="create_data_cluster",
        cluster_name="ephermal-spark-cluster-{{ds_nodash}}",
        master_machine_type="n1-standard-2",
        worker_machine_type="n1-standard-2",
        num_workers=2,
        region="us-central1",
        zone="us-central1-a",
        dag =dag
    )

    submit_pyspark = DataProcPySparkOperator(
        task_id="run_pyspark_etl",
        main=PYSPARK_JOB,
        cluster_name="ephermal-spark-cluster-{{ds_nodash}}",
        region="us-central1"
    )

    bq_load_delay_by_distance = GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_delays_by_distance",
        bucket='bigquery_peter',
        source_objects=["output-folder/" + current_date + "_bydistance/part-*"],
        destination_project_dataset_table="united-monument-360105.Practice.df_avg_delay_wrt_distance",
        autodetect=True,
        schema_fields=[{"name": "distance_catogory", "type": "NUMERIC", "mode": "NULLABLE"},
                       {"name": "avg_arrival_delay", "type": "NUMERIC", "mode": "NULLABLE"},
                       {"name": "avg_departure_delay", "type": "NUMERIC", "mode": "NULLABLE"}
                       ],
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name="ephermal-spark-cluster-{{ds_nodash}}",
        region="us-central1",
        trigger_rule=TriggerRule.ALL_DONE
    )
    start_python_job = BeamRunPythonPipelineOperator(
        task_id="start-python-job",
        py_file=GCS_PYTHON,
        py_options=[],
        pipeline_options={
            'output': GCS_OUTPUT,
        },
        py_requirements=['apache-beam[gcp]==2.21.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config={'location': 'europe-west3'},
    )



    create_cluster.dag = dag

    wait_for_file.set_downstream(create_cluster)
    create_cluster.set_downstream(submit_pyspark)
    submit_pyspark.set_downstream(bq_load_delay_by_distance)
    bq_load_delay_by_distance.set_downstream(delete_cluster)
