import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from trigger_job_util import TriggerJobUtil
from dag_arguments import DAGArgs
import Config
from airflow.operators import bash_operator


def daily_sync_etl():
    try:
        job_name = Config.job_name
        temp_location = Config.temp_location
        zone = Config.zone
        input_param_name = Config.input_param_name
        gcs_path = Config.template_gcs_path
        body = DAGArgs().get_daily_trigger_dataflow_body(job_name, temp_location, zone, input_param_name)
        TriggerJobUtil.trigger_job(gcs_path, body)
    except Exception as ex:
        print(['Exception', ex])


try:
    with DAG(
            'bigquery_to_gcs_bucket',
            default_args=DAGArgs().default_args,
            description='DAG for daily file creation in bucket',
            max_active_runs=1,
            concurrency=4,
            catchup=False,
            schedule_interval='@daily',
    ) as dag:
        commands = """
            export GOOGLE_APPLICATION_CREDENTIALS=/home/airflow/gcs/dags/cio-sea-team-lab-9e09db-0b2782fa290b.json
            gcloud auth activate-service-account --key-file=/home/airflow/gcs/dags/cio-sea-team-lab-9e09db-0b2782fa290b.json
            """

        ingest_and_process = bash_operator.BashOperator(
            task_id='ingest_and_process',
            bash_command=commands)

        task1 = DataFlowPythonOperator(
            task_id='daily-sync-etl',
            python_callable=daily_sync_etl,
            dag=dag)
except IndexError as ex:
    logging.debug("Exception", str(ex))
ingest_and_process >> task1