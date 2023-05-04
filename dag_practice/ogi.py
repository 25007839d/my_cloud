from airflow import DAG
import datetime
from datetime import timedelta
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator,DataprocClusterDeleteOperator,DataProcPySparkOperator

default_arg = {
            'start_date':datetime.datetime(2023,1,18),
            'retry':2,
            'retry_delay':timedelta(minutes=5),
            'catchup':False,
            'schedule_interval': "* * * * *"
              }




with DAG(dag_id="dataproc",default_args=default_arg) as dag:

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
        mum_masters=2,
        region="us-central1",
        zone="us-central1-a",
        dag=dag
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



}