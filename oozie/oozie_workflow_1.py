from datetime import timedelta
import os
import yaml
import sys
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, \
    DataprocSubmitHiveJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable



BASE_DIR = "/home/airflow/gcs/dags/vz-it-gudv-dtwndo-0"#--
sys.path.append(f"{BASE_DIR}/dags")



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
debug_enabled = config_values['debug_enabled']
frequency = config_values['frequency']
cellsite_add_partition_hql = config_values['cellsite_add_partition_hql']
pyfile_common_function = config_values['pyfile_common_function']
spark_external_avro_jar = config_values['spark_external_avro_jar']
spark_avro_jar=config_values['spark_avro_jar']
spark_xml_jar=config_values['spark_avro_jar']
CLUSTER_NAME = config_values['CLUSTER_NAME']
DAG_ID = config_values['DAG_ID']
base_directory = config_values['base_directory']
env = config_values['env']
main_uri_nokia_4G= config_values['main_uri_nokia_4G']
base_app_name= config_values['base_app_name']
aidpe_core_dataset= config_values['base_app_name']
digital_twin_ran_inventory_aidpe_quarantine= config_values['digital_twin_ran_inventory_aidpe_quarantine']
VzwRanDimInventoryRadioDevicesNorm_dataset_name=['VzwRanDimInventoryRadioDevicesNorm_dataset_name']
VzwRanDimInventoryBbuDevicesNorm_dataset_name=['VzwRanDimInventoryBbuDevicesNorm_dataset_name']
VzwRanDimInventoryRadioCellDevicesNorm_dataset_name=['VzwRanDimInventoryRadioCellDevicesNorm_dataset_name']
VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_dataset_name=['VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_dataset_name']
VzwRanDimInventoryEnodB_dataset_name=['VzwRanDimInventoryEnodB_dataset_name']
VzwRanDimInventoryRadioDevicesNorm_table_name=['VzwRanDimInventoryRadioDevicesNorm_table_name']
VzwRanDimInventoryBbuDevicesNorm_table_name=['VzwRanDimInventoryBbuDevicesNorm_table_name']
VzwRanDimInventoryRadioCellDevicesNorm_table_name=config_values['VzwRanDimInventoryRadioCellDevicesNorm_table_name']
VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_table_name=config_values['VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_table_name']
VzwRanDimInventoryEnodB_table_name=config_values['VzwRanDimInventoryEnodB_table_name']
pipeline_vendor_name=config_values['pipeline_vendor_name']
pipeline_4g_lte_technology=config_values['pipeline_4g_lte_technology']
pipeline_5g_uwb_technology=config_values['pipeline_5g_uwb_technology']
pipeline_5g_nw_technology= config_values['pipeline_5g_nw_technology']
trans_dt=config_values['trans_dt']
app_version= config_values['app_version']
src= config_values['src']
run_date= config_values['run_date']
log_file= config_values['log_file']
inventory_app_version= config_values['inventory_app_version']
output_dir= config_values['output_dir']
lof_output_file_location=['lof_output_file_location']
marketName= config_values['marketName']
kafka_switchx_nokia_onair_table_name= config_values['kafka_switchx_nokia_onair_table_name']
kafka_switchx_nokia_market_dataset_dir= config_values['kafka_switchx_nokia_market_dataset_dir']
dataset_switchx_nokia_reference= config_values['dataset_switchx_nokia_reference']
aidpe_core_dataset_daily= config_values['aidpe_core_dataset_daily']
reconciliation_days= config_values['reconciliation_days']
market_exists=config_values['market_exists']
file_path= config_values['file_path']
dataset_antenna_reference=config_values['dataset_antenna_reference']
spark_external_jars= config_values['spark_external_jars']
main_uri_nokia_5GUWB= config_values['main_uri_nokia_5GUWB']
main_uri_nokia_5GNW= config_values['main_uri_nokia_5GNW']
main_hive_file_uri= config_values['main_hive_file_uri']
hive_db_base_app_name= config_values['hive_db_base_app_name']
hive_db_quar= config_values['hive_db_quar']
TRANS_DT=config_values['TRANS_DT']
VENDOR= config_values['VENDOR']
MARKET= config_values['MARKET']
add_partition_4G_LTE_hql= config_values['add_partition_4G_LTE_hql']
add_partition_5G_NW_hql= config_values['add_partition_5G_NW_hql']
add_partition_5G_UWB_hql= config_values['add_partition_5G_UWB_hql']
get_trans_dt_py= config_values['get_trans_dt_.py']
technology_type = config_values['technology_type']
threshold_days = config_values['threshold_days']
load_date = config_values['load_date']
hive_db = config_values['hive_db']
pipeline_4g_lte_technolog = config_values['pipeline_4g_lte_technolog']
outputFile = config_values['outputFile']

file_path_get = Variable.get("file_path")
trans_dt_get =Variable.get("trans_dt")
market_exists_get =Variable.get("market_exists")

nominal_time = "{{ (execution_date).strftime(\"%Y%m%d\") }}"

PYSPARK_JOB_PROPERTIES = {
    'spark.driver.memory': '48g',
    'spark.submit.deployMode': 'cluster',
    'spark.executor.memoryOverhead': '2g',
    'spark.executor.cores': '3',
    'spark.executor.memory': '32g',
    'spark.shuffle.service.enabled': 'true',
    'spark.dynamicAllocation.executorIdleTimeout': '60',
    'spark.dynamicAllocation.initialExecutors': '2',
    'spark.dynamicAllocation.minExecutors':'2',
    'spark.dynamicAllocation.maxExecutors': '1000',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.sql.broadcastTimeout':'3600',
    'spark.sql.adaptive.enabled':'false',
    # 'mapred:mapreduce.input.fileinputformat.split.minsize': '240000000',
    'spark.master': 'yarn',
    # 'spark.driver.userClassPathFirst': 'true',
    # 'spark.executor.userClassPathFirst': 'true',
    # 'spark.yarn.maxAppAttempts': '1'
}

PYSPARK_JOB_spark_node_4G = {
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": main_uri_nokia_4G,
                    "properties": PYSPARK_JOB_PROPERTIES,
                    "jar_file_uris": [f"{spark_external_jars}/{spark_avro_jar}",f"{spark_external_jars}/${spark_xml_jar}"],
                    "python_file_uris": [pyfile_common_function],
                    "args":{'base_app_name': base_app_name,
                            'env':'{env}',
                            'core_dataset':aidpe_core_dataset,
                            'quarantine':digital_twin_ran_inventory_aidpe_quarantine,
                            'VzwRanDimInventoryRadioDevicesNorm_dataset':VzwRanDimInventoryRadioDevicesNorm_dataset_name,
                            'VzwRanDimInventoryBbuDevicesNorm_dataset':VzwRanDimInventoryBbuDevicesNorm_dataset_name,
                            'VzwRanDimInventoryRadioCellDevicesNorm_dataset':VzwRanDimInventoryRadioCellDevicesNorm_dataset_name,
                            'VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_dataset':VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_dataset_name,
                            'VzwRanDimInventoryEnodB_dataset':VzwRanDimInventoryEnodB_dataset_name,
                            'VzwRanDimInventoryRadioDevicesNorm_table':VzwRanDimInventoryRadioDevicesNorm_table_name,
                            'VzwRanDimInventoryBbuDevicesNorm_table':VzwRanDimInventoryBbuDevicesNorm_table_name,
                            'VzwRanDimInventoryRadioCellDevicesNorm_table':VzwRanDimInventoryRadioCellDevicesNorm_table_name,
                            'VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_table':VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_table_name,
                            'VzwRanDimInventoryEnodB_table':VzwRanDimInventoryEnodB_table_name,
                            'vendor':pipeline_vendor_name,
                            'technology':pipeline_4g_lte_technology,
                            'trans_dt':trans_dt_get,#
                            'src':src,
                            'run_date':run_date,
                            'log_level':log_file,
                            'app_version':inventory_app_version,
                            'output_dir':output_dir,
                            'lof_output_file_location':lof_output_file_location,
                            'marketName':marketName,
                            'onair_table_name':kafka_switchx_nokia_onair_table_name,
                            'dataset_market_reference':kafka_switchx_nokia_market_dataset_dir,
                            'dataset_switchx_nokia_reference':dataset_switchx_nokia_reference,
                            'core_dataset_daily':aidpe_core_dataset_daily,
                            'reconciliation_days_BbuDevicesNorm':reconciliation_days,
                            'reconciliation_days_RadioDevicesNorm':reconciliation_days,
                            'reconciliation_days_RadioCellDevicesNorm':reconciliation_days,
                            'reconciliation_days_RadioBbuAntennaAssignmentNorm':reconciliation_days,
                            'reconciliation_days_EnodBNorm':reconciliation_days,
                            'bootstrap_enabled_BbuDevicesNorm':False,
                            'bootstrap_enabled_RadioDevicesNorm':False,
                            'bootstrap_enabled_RadioCellDevicesNorm':False,
                            'bootstrap_enabled_RadioBbuAntennaAssignmentNorm':False,
                            'bootstrap_enabled_EnodBNorm':False,
                            'market_exists':market_exists_get,#
                            'file_path': file_path_get,#
                            'dataset_antenna_reference':dataset_antenna_reference
                            }
                                        }
}

PYSPARK_JOB_spark_node_5GUWB = {
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": main_uri_nokia_5GUWB,
                    "properties": PYSPARK_JOB_PROPERTIES,
                    "jar_file_uris": [f"{spark_external_jars}/{spark_avro_jar}",f"{spark_external_jars}/${spark_xml_jar}"],
                    "python_file_uris": [pyfile_common_function],
                    "args":{'base_app_name': base_app_name,
                          'env':env,
                          'core_dataset':aidpe_core_dataset,
                          'quarantine':digital_twin_ran_inventory_aidpe_quarantine,
                          'VzwRanDimInventoryRadioDevicesNorm_dataset':VzwRanDimInventoryRadioDevicesNorm_dataset_name,
                          'VzwRanDimInventoryBbuDevicesNorm_dataset':VzwRanDimInventoryBbuDevicesNorm_dataset_name,
                          'VzwRanDimInventoryRadioCellDevicesNorm_dataset':VzwRanDimInventoryRadioCellDevicesNorm_dataset_name,
                          'VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_dataset':VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_dataset_name,
                          'VzwRanDimInventoryEnodB_dataset':VzwRanDimInventoryEnodB_dataset_name,
                          'VzwRanDimInventoryRadioDevicesNorm_table':VzwRanDimInventoryRadioDevicesNorm_table_name,
                          'VzwRanDimInventoryBbuDevicesNorm_table':VzwRanDimInventoryBbuDevicesNorm_table_name,
                          'VzwRanDimInventoryRadioCellDevicesNorm_table':VzwRanDimInventoryRadioCellDevicesNorm_table_name,
                          'VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_table':VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_table_name,
                          'VzwRanDimInventoryEnodB_table':VzwRanDimInventoryEnodB_table_name,
                          'vendor':pipeline_vendor_name,
                          'technology':pipeline_5g_uwb_technology,
                          'trans_dt':trans_dt_get,
                          'src':src,
                          'run_date':run_date
                          'log_level':log_file,
                          'app_version':app_version,
                          'output_dir':output_dir,
                          'lof_output_file_location':lof_output_file_location,
                          'marketName':marketName,
                          'onair_table_name':kafka_switchx_nokia_onair_table_name,
                          'dataset_market_reference':kafka_switchx_nokia_market_dataset_dir,
                          'dataset_switchx_nokia_reference':dataset_switchx_nokia_reference,
                          'core_dataset_daily':aidpe_core_dataset_daily,
                          'reconciliation_days_BbuDevicesNorm':reconciliation_days,
                          'reconciliation_days_RadioDevicesNorm':reconciliation_days,
                          'reconciliation_days_RadioCellDevicesNorm':reconciliation_days,
                          'reconciliation_days_RadioBbuAntennaAssignmentNorm':reconciliation_days,
                          'reconciliation_days_EnodBNorm':reconciliation_days,
                          'bootstrap_enabled_BbuDevicesNorm':False,
                          'bootstrap_enabled_RadioDevicesNorm':False,
                          'bootstrap_enabled_RadioCellDevicesNorm':False,
                          'bootstrap_enabled_RadioBbuAntennaAssignmentNorm':False,
                          'bootstrap_enabled_EnodBNorm':False,
                          'market_exists':market_exists_get,
                          'file_path':file_path_get

                                                }
                                        }
}

PYSPARK_JOB_spark_node_5GNW = {
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": main_uri_nokia_5GNW,
                    "properties": PYSPARK_JOB_PROPERTIES,
                    "jar_file_uris": [f"{spark_external_jars}/{spark_avro_jar}",f"{spark_external_jars}/${spark_xml_jar}"],
                    "python_file_uris": [pyfile_common_function],
                    "args": {'base_app_name': base_app_name,
                             'env': env,
                             'core_dataset': aidpe_core_dataset,
                             'quarantine': digital_twin_ran_inventory_aidpe_quarantine,
                             'VzwRanDimInventoryRadioDevicesNorm_dataset': VzwRanDimInventoryRadioDevicesNorm_dataset_name,
                             'VzwRanDimInventoryBbuDevicesNorm_dataset': VzwRanDimInventoryBbuDevicesNorm_dataset_name,
                             'VzwRanDimInventoryRadioCellDevicesNorm_dataset': VzwRanDimInventoryRadioCellDevicesNorm_dataset_name,
                             'VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_dataset': VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_dataset_name,
                             'VzwRanDimInventoryEnodB_dataset': VzwRanDimInventoryEnodB_dataset_name,
                             'VzwRanDimInventoryRadioDevicesNorm_table': VzwRanDimInventoryRadioDevicesNorm_table_name,
                             'VzwRanDimInventoryBbuDevicesNorm_table': VzwRanDimInventoryBbuDevicesNorm_table_name,
                             'VzwRanDimInventoryRadioCellDevicesNorm_table': VzwRanDimInventoryRadioCellDevicesNorm_table_name,
                             'VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_table': VzwRanDimInventoryRadioBbuAntennaAssignmentNorm_table_name,
                             'VzwRanDimInventoryEnodB_table': VzwRanDimInventoryEnodB_table_name,
                             'vendor': pipeline_vendor_name,
                             'technology': pipeline_5g_nw_technology,
                             'trans_dt': trans_dt_get,
                             'src': src,
                             'run_date': run_date
                             'log_level': log_file,
                             'app_version': app_version,
                             'output_dir': output_dir,
                             'lof_output_file_location': lof_output_file_location,
                             'marketName': marketName,
                             'onair_table_name': kafka_switchx_nokia_onair_table_name,
                             'dataset_market_reference': kafka_switchx_nokia_market_dataset_dir,
                             'dataset_switchx_nokia_reference': dataset_switchx_nokia_reference,
                             'core_dataset_daily': aidpe_core_dataset_daily,
                             'reconciliation_days_BbuDevicesNorm': reconciliation_days,
                             'reconciliation_days_RadioDevicesNorm': reconciliation_days,
                             'reconciliation_days_RadioCellDevicesNorm': reconciliation_days,
                             'reconciliation_days_RadioBbuAntennaAssignmentNorm': reconciliation_days,
                             'reconciliation_days_EnodBNorm': reconciliation_days,
                             'bootstrap_enabled_BbuDevicesNorm': False,
                             'bootstrap_enabled_RadioDevicesNorm': False,
                             'bootstrap_enabled_RadioCellDevicesNorm': False,
                             'bootstrap_enabled_RadioBbuAntennaAssignmentNorm': False,
                             'bootstrap_enabled_EnodBNorm': False,
                             'market_exists': market_exists_get,
                             'file_path': file_path_get

                             }
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

get_trans_dt = BashOperator(
    task_id="get_trans_dt",
    bash_command=f"python {get_trans_dt_py} {outputFile} {marketName} {trans_dt} {env} {pipeline_vendor_name}",
    dag=dag)



spark_node_4G = DataprocSubmitJobOperator(
    task_id="spark_node_4G",
    job=PYSPARK_JOB_spark_node_4G,
    region=REGION,
    project_id=GCP_PROJECT_ID,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,

)
spark_node_5GUWB = DataprocSubmitJobOperator(
    task_id="spark_node_5GUWB",
    job=PYSPARK_JOB_spark_node_5GUWB,
    region=REGION,
    project_id=GCP_PROJECT_ID,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)
spark_node_5GNW = DataprocSubmitJobOperator(
    task_id="spark_node_5GNW",
    job=PYSPARK_JOB_spark_node_5GNW,
    region=REGION,
    project_id=GCP_PROJECT_ID,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)


add_partition_4G_LTE = DataprocSubmitHiveJobOperator(
    task_id='add_partition_4G_LTE',
    gcp_conn_id=GCP_CONN_ID,
    query_uri=add_partition_4G_LTE_hql,
    variables={"HIVE_DB": hive_db,
               "HIVE_DB_QUAR": hive_db_quar,
               "TRANS_DT": trans_dt_get,
               "TECHNOLOGY":pipeline_4g_lte_technolog,
               "MARKET":marketName,
               "DATESTAMP":run_date},
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag
)

add_partition_5G_NW = DataprocSubmitHiveJobOperator(
    task_id='add_partition_5G_NW',
    gcp_conn_id=GCP_CONN_ID,
    query_uri=add_partition_5G_NW_hql,
    variables={"HIVE_DB": hive_db,
               "HIVE_DB_QUAR": hive_db_quar,
               "TRANS_DT": trans_dt_get,
               "TECHNOLOGY":pipeline_5g_nw_technology,
               "MARKET":marketName,
               "DATESTAMP":run_date},
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag
)
add_partition_5G_UWB = DataprocSubmitHiveJobOperator(
    task_id='add_partition_5G_UWB',
    gcp_conn_id=GCP_CONN_ID,
    query_uri=add_partition_5G_UWB_hql,
    variables={"HIVE_DB": hive_db,
               "HIVE_DB_QUAR": hive_db_quar,
               "TRANS_DT": trans_dt_get,
               "TECHNOLOGY":pipeline_5g_uwb_technology,
               "MARKET":marketName,
               "DATESTAMP":run_date},
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,

)


end = DummyOperator(task_id='end', dag=dag)



get_trans_dt>>spark_node_4G,spark_node_5GUWB,spark_node_5GNW>>add_partition_4G_LTE>>add_partition_5G_NW>>add_partition_5G_UWB>>end
