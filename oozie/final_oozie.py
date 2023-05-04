from datetime import timedelta
import os
import yaml
import sys
from datetime import datetime
from time import sleep
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.operators.python import BranchPythonOperator
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator,DataprocSubmitHiveJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from google.cloud import storage
from airflow.operators.python_operator import PythonOperator


load_date = '{{ ds }}'
BASE_DIR = "/home/airflow/gcs/dags/vz-it-gudv-dtwndo-0"
# JOB_NAME = "wap_base_acct_profile"
# PROCESS_NAME = "base_acct_profile"
sys.path.append(f"{BASE_DIR}/dags")
# from cdh_mail import notify_failed_mail
GCP_PROJECT_ID = ""
GCP_CONN_ID = ""
REGION = ""
CLUSTER_NAME = ""
DAG_ID = ""
base_directory = ""
env = ""
PYSPARK_URI = ""
base_app_name = ""
aidpe_core_dataset_root = ""
dtwn_ran_inv_ref_hive_db_quar = ""
dtwn_ran_inv_ref_hive_db = ""
antenna_add_partition_hql = ""
pyfile_common_function = ""
spark_external_avro_jar = ""
dataset_antenna_core_path = ""
dtwn_ran_inv_ref_diminvantdvsnorm_tab_name = ""
dataset_antenna_core_path_daily = ""
dataset_antenna_quarantine_path = ""
dtwn_ran_inv_ref_diminvantdvsnorm_ds_name = ""
aidpe_quarantine_root = ""
frequency = ""
gcs_bucket = ""
context_hive_ddl_success_file_root = ""
antenna_root_directory = ""


# Read the YML file and pass the respective parameters based on GCP project
project = os.environ['GCP_PROJECT']
with open(f"{BASE_DIR}/digital_twin_ran_inventory_reference/config/base_conf.yml", 'r') as file:
    base_config = yaml.full_load(file)

with open(f"{BASE_DIR}/digital_twin_ran_inventory_reference/config/digital_twin_ran_inventory_reference_antenna_conf.yml", 'r') as file:
    dag_config = yaml.full_load(file)

config_values = {}

filtered_base_dict = dict(filter(lambda elem: elem[0] == project, base_config.items()))
filtered_dict = dict(filter(lambda elem: elem[0] == project, dag_config.items()))

if len(filtered_base_dict) > 0:
    base_value = filtered_base_dict[project][0]
    config_values = {**config_values, **base_value}
else:
    print("No config found exiting..")
    sys.exit(-1)
if len(filtered_dict) > 0:
    app_value = filtered_dict[project][0]
    config_values = {**config_values, **app_value}
else:
    print("No config found exiting..")
    sys.exit(-1)

GCP_PROJECT_ID = config_values['gcp_project']
GCP_CONN_ID = config_values['google_cloud_conn_id']
REGION = config_values['region']
CLUSTER_NAME = config_values['spark_cluster']
DAG_ID = config_values['dag_id']
base_directory = config_values['base_directory']
env = config_values['env']
PYSPARK_URI = config_values["python_file"]
base_app_name = config_values['base_app_name']
aidpe_core_dataset_root = config_values['aidpe_core_dataset_root']
dtwn_ran_inv_ref_hive_db_quar = config_values['dtwn_ran_inv_ref_hive_db_quar']
dtwn_ran_inv_ref_hive_db = config_values['dtwn_ran_inv_ref_hive_db']
antenna_add_partition_hql = config_values['antenna_root_directory'] + "/hive/add_partitions.hql"
pyfile_common_function = config_values['antenna_root_directory'] + "/python/common_functions.py"
spark_external_avro_jar = config_values['spark_external_avro_jar']
dataset_antenna_core_path = config_values['dataset_antenna_core_path']
dataset_antenna_core_path_daily = config_values['dataset_antenna_core_path_daily']
dataset_antenna_quarantine_path = config_values['dataset_antenna_quarantine_path']
dtwn_ran_inv_ref_diminvantdvsnorm_tab_name = config_values['dtwn_ran_inv_ref_diminvantdvsnorm_tab_name']
dtwn_ran_inv_ref_diminvantdvsnorm_ds_name = config_values['dtwn_ran_inv_ref_diminvantdvsnorm_ds_name']
aidpe_quarantine_root = config_values['aidpe_quarantine_root']
frequency = config_values['frequency']
gcs_bucket = config_values['gcs_bucket']
context_hive_ddl_success_file_root = config_values['context_hive_ddl_success_file_root']
antenna_root_directory = config_values['antenna_root_directory']



run_date = "{{ (execution_date).strftime(\"%Y%m%d\") }}"
nominal_time = "{{ (execution_date).strftime(\"%Y%m%d\") }}"

current_time = datetime.now()
previous_day_time = current_time-timedelta(days=1)

current_time1 = current_time.strftime("%Y%m%d")
previous_day = previous_day_time.strftime("%d-%m-%Y")

check_path_1 = f"{context_hive_ddl_success_file_root}/{run_date}/{dtwn_ran_inv_ref_diminvantdvsnorm_ds_name}/_SUCCESS"

PYSPARK_JOB_PROPERTIES = {
    'spark.driver.memory': '8g',
    'spark.submit.deployMode': 'cluster',
    'spark.executor.memoryOverhead': '2g',
    'spark.executor.cores': '3',
    'spark.executor.memory': '4g',
    'spark.shuffle.service.enabled': 'true',
    'spark.dynamicAllocation.executorIdleTimeout': '60',
    'spark.dynamicAllocation.initialExecutors': '2',
    'spark.dynamicAllocation.maxExecutors': '10',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'mapred:mapreduce.input.fileinputformat.split.minsize': '240000000',
    'spark.dynamicAllocation.minExecutors': '2',
    'spark.master': 'yarn',
    'spark.driver.userClassPathFirst': 'true',
    'spark.executor.userClassPathFirst': 'true',
    'spark.yarn.maxAppAttempts': '1'
}

PYSPARK_JOB = {
"placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI,
                    "properties": PYSPARK_JOB_PROPERTIES,
                    "jar_file_uris": [spark_external_avro_jar],
                    "python_file_uris": [pyfile_common_function],
                    "args": {"base_app_name": base_app_name,
                             "env": env,
                             "aidpe_core_dataset_root": aidpe_core_dataset_root,
                             "aidpe_quarantine_root": aidpe_quarantine_root,
                             "VzwRanDimInventoryAntennaDevicesNorm_dataset": dtwn_ran_inv_ref_diminvantdvsnorm_ds_name,
                             "src": dtwn_ran_inv_ref_diminvantdvsnorm_ds_name,
                             "VzwRanDimInventoryAntennaDevicesNorm_table": dtwn_ran_inv_ref_diminvantdvsnorm_tab_name,
                             "hive_db": dtwn_ran_inv_ref_hive_db,
                             "run_date": run_date,
                             "trans_dt": nominal_time,
                             "antenna_core_path": dataset_antenna_core_path,
                             "antenna_core_path_daily": dataset_antenna_core_path_daily,
                             "antenna_quarantine_path": dataset_antenna_quarantine_path,
                             "bootstrap_enabled": False,
                             "reconciliation_days": "7"}
                    }
}



def get_dag_state(**kwargs):
    from google.cloud import storage
    client = storage.Client()

    bucket = client.get_bucket(kwargs['bucket_name'])

    blobs = bucket.list_blobs()

    for blob in blobs:
        if kwargs['prefix'] == blob.name:
            print(blob.name)
            return "run_spark_antenna"

        for i in range(1,5):
            second =  i*60
            from time import sleep
            sleep(second)
            blobs = bucket.list_blobs()
            for blob in blobs:
                if kwargs['prefix'] == blob.name:
                    print(blob.name)
                    return "run_spark_antenna"

        else:

            return 'end1'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,9,14),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval=frequency,
    catchup=False,
    default_args=default_args,
    concurrency=3,
    tags= ["dtwin", "SM", "RK"]
)

start = DummyOperator(task_id='start', dag=dag)



check_path = BranchPythonOperator(
    task_id='check_path',
    python_callable=get_dag_state,
    dag=dag,
    op_kwargs={"bucket_name":gcs_bucket,'prefix':check_path_1})

run_spark_antenna = DataprocSubmitJobOperator(
    task_id="run_spark_antenna",
    job=PYSPARK_JOB,
    region=REGION,
    project_id=GCP_PROJECT_ID,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

add_partition = DataprocSubmitHiveJobOperator(
    task_id='add_partition_antenna',
    gcp_conn_id=GCP_CONN_ID,
    query_uri=antenna_add_partition_hql,
    variables={"HIVE_DB": dtwn_ran_inv_ref_hive_db,
               "HIVE_DB_QUAR": dtwn_ran_inv_ref_hive_db_quar,
               "TRANS_DT": nominal_time},
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)
end1 = DummyOperator(task_id='end1', dag=dag)

start >> check_path >> run_spark_antenna >> add_partition >> end
start >> check_path>> end1
