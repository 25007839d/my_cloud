from datetime import timedelta
import os
import yaml
import sys
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, \
    DataprocSubmitHiveJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor


load_date = '{{ ds }}'
BASE_DIR = "/home/airflow/gcs/dags/vz-it-gudv-dtwndo-0"
# JOB_NAME = "wap_base_acct_profile"
# PROCESS_NAME = "base_acct_profile"
sys.path.append(f"{BASE_DIR}/dags")
# from cdh_mail import notify_failed_mail
cellsite_add_partition_hql = ""
pyfile_common_function = ""
spark_external_avro_jar = ""
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
aidpe_quarantine_root = ""
enodeb_site_map_src = ""
cell_site_twin_src = ""
enodeb_site_map_table = ""
cell_site_twin_table = ""
hive_db = ""
hive_db_quar = ""
vzn_ndl_vpi_core_tbls_hive_db = ""
vzn_ndl_fuze_core_tbls_hive_db = ""
vzn_ndl_granite_core_tbls_hive_db = ""
vendor_xref = ""
vzwn_nsdl_ran_core_tbls_hive_db = ""
onair_site_daily_table = ""
site_structure_table = ""
circuit_info_raw = ""
site_info_raw = ""
radio_bbu_antenna_assignment_table = ""
debug_enabled = ""
technology_type = ""
threshold_days = ""
aidpe_core_dataset_daily_root = ""
reconciliation_days_cell_site = ""
bootstrap_enabled_cell_site = ""
bootstrap_enabled_enodeb_site_map = ""
reconciliation_days_enodeb_site_map = ""
run_date = "{{ (execution_date).strftime(\"%Y%m%d\") }}"
frequency = ""

# Read the YML file and pass the respective parameters based on GCP project
project = os.environ['GCP_PROJECT']
with open(f"{BASE_DIR}/config/base_conf.yml", 'r') as file:
    base_config = yaml.full_load(file)

with open(f"{BASE_DIR}/config/digital_twin_ran_inventory_reference_cellsite_conf.yml", 'r') as file:
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
aidpe_quarantine_root = config_values['aidpe_quarantine_root']
enodeb_site_map_src = config_values['enodeb_site_map_src']
cellsite_add_partition_hql = config_values['base_directory'] + "/hive/add_partitions.hql"
pyfile_common_function = config_values['base_directory'] + "/python/common_functions.py"
spark_external_avro_jar = config_values['spark_external_avro_jar']
cell_site_twin_src = config_values['cell_site_twin_src']
enodeb_site_map_table = config_values['enodeb_site_map_table']
cell_site_twin_table = config_values['cell_site_twin_table']
hive_db = config_values['hive_db']
hive_db_quar = config_values['hive_db_quar']
vzn_ndl_vpi_core_tbls_hive_db = config_values['vzn_ndl_vpi_core_tbls_hive_db']
vzn_ndl_fuze_core_tbls_hive_db = config_values['vzn_ndl_fuze_core_tbls_hive_db']
vzn_ndl_granite_core_tbls_hive_db = config_values['vzn_ndl_granite_core_tbls_hive_db']
vendor_xref = config_values['vendor_xref']
vzwn_nsdl_ran_core_tbls_hive_db = config_values['vzwn_nsdl_ran_core_tbls_hive_db']
onair_site_daily_table = config_values['onair_site_daily_table']
site_structure_table = config_values['site_structure_table']
circuit_info_raw = config_values['circuit_info_raw']
site_info_raw = config_values['site_info_raw']
radio_bbu_antenna_assignment_table = config_values['radio_bbu_antenna_assignment_table']
debug_enabled = config_values['debug_enabled']
aidpe_core_dataset_daily_root = config_values['aidpe_core_dataset_daily_root']
frequency = config_values['frequency']
technology_type = ['technology_type']
threshold_days = ['threshold_days']
reconciliation_days_cell_site = ['reconciliation_days_cell_site']
bootstrap_enabled_cell_site = ['bootstrap_enabled_cell_site']
bootstrap_enabled_enodeb_site_map = ['bootstrap_enabled_enodeb_site_map']
reconciliation_days_enodeb_site_map = ['reconciliation_days_enodeb_site_map']


run_date = "{{ (execution_date).strftime(\"%Y%m%d\") }}"
nominal_time = "{{ (execution_date).strftime(\"%Y%m%d\") }}"

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
                             "run_date": run_date,
                             "enodeb_site_map_src": enodeb_site_map_src,
                             "cell_site_twin_src": cell_site_twin_src,
                             "enodeb_site_map_table": enodeb_site_map_table,
                             "cell_site_twin_table": cell_site_twin_table,
                             "hive_db": hive_db,
                             "hive_db_quar": hive_db_quar,
                             "vzn_ndl_vpi_core_tbls_hive_db": vzn_ndl_vpi_core_tbls_hive_db,
                             "vzn_ndl_fuze_core_tbls_hive_db": vzn_ndl_fuze_core_tbls_hive_db,
                             "vzn_ndl_granite_core_tbls_hive_db": vzn_ndl_granite_core_tbls_hive_db,
                             "vendor_xref": vendor_xref,
                             "vzwn_nsdl_ran_core_tbls_hive_db": vzwn_nsdl_ran_core_tbls_hive_db,
                             "onair_site_daily_table": onair_site_daily_table,
                             "site_structure_table": site_structure_table,
                             "circuit_info_raw": circuit_info_raw,
                             "site_info_raw": site_info_raw,
                             "radio_bbu_antenna_assignment_table": radio_bbu_antenna_assignment_table,
                             "debug_enabled": debug_enabled,
                             "technology_type": technology_type,
                             "threshold_days": threshold_days,
                             "aidpe_core_dataset_daily_root": aidpe_core_dataset_daily_root,
                             "reconciliation_days_cell_site": reconciliation_days_cell_site,
                             "bootstrap_enabled_cell_site": bootstrap_enabled_cell_site,
                             "bootstrap_enabled_enodeb_site_map": bootstrap_enabled_enodeb_site_map,
                             "reconciliation_days_enodeb_site_map": reconciliation_days_enodeb_site_map}
                    }
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 14),
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
    tags= [run_date, nominal_time]
)
# look success file 2 file
wait_for_file = GCSObjectExistenceSensor(
        task_id='wait_for_file',
        bucket='bigquery_peter',
        object=current_date + '.csv',
        google_cloud_conn_id='google_cloud_storage_default'
                                            )

start = DummyOperator(task_id='start', dag=dag)

run_spark_cellsite = DataprocSubmitJobOperator(
    task_id="run_spark_antenna",
    job=PYSPARK_JOB,
    region=REGION,
    project_id=GCP_PROJECT_ID,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

add_partition = DataprocSubmitHiveJobOperator(
    task_id='add_partition_cellsite',
    gcp_conn_id=GCP_CONN_ID,
    query_uri=cellsite_add_partition_hql,
    variables={"HIVE_DB": hive_db,
               "HIVE_DB_QUAR": hive_db_quar,
               "TRANS_DT": nominal_time},
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag
)

# two success file flag create 2 file

"gs://gudv-dev-dtwndo-0-usmr-warehouse/dtwin/vzw/internal/ran_hive_ddl_success/${YEAR}${MONTH}${DAY}/vzw.ran.dim.inventory.enodeb_site_map.norm.v0"
"gs://gudv-dev-dtwndo-0-usmr-warehouse/dtwin/vzw/internal/ran_hive_ddl_success/${YEAR}${MONTH}${DAY}/vzw.ran.dim.inventory.cell_sites.norm.v0"

end = DummyOperator(task_id='end', dag=dag)



start >> run_spark_cellsite >> add_partition >> end
