"""
DAGs for ETL common pipeline
"""

import logging
import os
from typing import Dict, Any
from uuid import uuid4

import pendulum
import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from telus.etl_metadata_utils.db import Connectors, ProcessStatus

from utils.common_operators import AddProcessStatusOperator
from utils.etl_metadata import ModelFactoryWrapper, add_process_status, SourceCodes
from utils.render_template import render

PROJECT_ID = os.getenv("PROJECT")
LOCATION = os.getenv("LOCATION")
NETWORK = os.getenv("NETWORK")
SUBNETWORK = os.getenv("SUBNETWORK")

RUN_DATE = "{{ ds_nodash }}"
RUN_TIME = "{{ ts_nodash }}"
PROCESS_ID = "{{ task_instance.xcom_pull('create_process_id')['process_id'] }}"
METADATA_DB_HOST = os.getenv("ETL_DB")

base_path = os.path.dirname(__file__)
template_search_path = [os.path.join(base_path, "templates", "dataflow")]

# Load configuration
with open(os.path.join(base_path, "config/common_pipeline.yaml")) as fl:
    config: Dict[str, Any] = yaml.safe_load(fl)
    metadata_db_config: Dict[str, Any] = config["metadata_db"]

timezone = pendulum.timezone(config.get("timezone", "UTC"))

model_factory_wrapper = ModelFactoryWrapper(
    project_id=PROJECT_ID,
    host=METADATA_DB_HOST,
    port=metadata_db_config["port"],
    db_name=metadata_db_config["db_name"],
    user_secret_name=metadata_db_config["user_secret"],
    pass_secret_name=metadata_db_config["pass_secret"],
    connector_type=Connectors[metadata_db_config["connector_type"]]
)


def on_dataflow_job_failure_callback(context):
    ti = context['task_instance']
    process_id = ti.xcom_pull(task_ids="create_process_id", key="process_id")
    logging.info(f"Dataflow job failed. Updating process status to FAILED.")
    add_process_status(model_factory_wrapper=model_factory_wrapper,
                       process_id=process_id, process_status=ProcessStatus.FAILED)


def on_bq_load_job_failure_callback(context):
    ti = context['task_instance']
    process_id = ti.xcom_pull(task_ids="create_process_id", key="process_id")
    logging.info(f"BigQuery Load job failed. Updating process status to FAILED.")
    add_process_status(model_factory_wrapper=model_factory_wrapper,
                       process_id=process_id, process_status=ProcessStatus.FAILED)


@task(multiple_outputs=True)
def create_process_id():
    process_id = str(uuid4())
    return {"process_id": process_id}


def create_dataflow_job_task(dataflow_config: Dict[str, Any],
                             dataflow_template: Dict[str, Any]):

    # Get templated launch request body
    launch_request_body = dataflow_template["launch_request"]

    return DataflowStartFlexTemplateOperator(
        task_id="run_dataflow_job",
        project_id=PROJECT_ID,
        location=LOCATION,
        body=launch_request_body,
        retries=dataflow_config.get("retries", 0),
        on_failure_callback=on_dataflow_job_failure_callback,
    )


def create_bq_load_task(bq_load_config: Dict[str, Any],
                        source_bucket: str,
                        source_folder: str):

    dataset_id = bq_load_config["dataset"]
    table_id = bq_load_config["table_name"]
    table_full_name = f"{PROJECT_ID}:{dataset_id}.{table_id}"

    schema_bucket = bq_load_config["schema_bucket"]
    schema_object = bq_load_config["schema_object"]
    source_objects = [f"{source_folder}/*.json"]

    return GCSToBigQueryOperator(
        task_id='load_data_to_bq',
        source_format="NEWLINE_DELIMITED_JSON",
        bucket=source_bucket,
        source_objects=source_objects,
        schema_object_bucket=schema_bucket,
        schema_object=schema_object,
        destination_project_dataset_table=table_full_name,
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_APPEND",
        max_bad_records=0,
        ignore_unknown_values=False,
        retries=bq_load_config.get("retries", 0),
        on_failure_callback=on_bq_load_job_failure_callback,
    )


def create_dag(dag_id: str, source_code: SourceCodes, pipeline_config: Dict[str, Any]):

    dataflow_config = pipeline_config["dataflow"]

    bq_load_config = pipeline_config["bq_load"]
    bq_load_config["schema_bucket"] = f"bq-schemas-{PROJECT_ID}"

    data_output_folder = f"{source_code.value}/{RUN_DATE}/{PROCESS_ID}"

    dataflow_template = render(
        search_path=template_search_path,
        template_name="common_pipeline_dataflow.yaml",
        data={
            "PROCESS_ID": PROCESS_ID,
            "SOURCE_CODE": source_code.value,
            "RUN_DATE": RUN_DATE,
            "PROJECT_ID": PROJECT_ID,
            "LOCATION": LOCATION,
            "NETWORK": NETWORK,
            "SUBNETWORK": SUBNETWORK,
            "CONFIG_NAME": dataflow_config["config_name"],
            "LOG_LEVEL": dataflow_config.get("log_level", "INFO"),
        }
    )

    schedule = pipeline_config["schedule"]

    dag = DAG(dag_id=dag_id,
              schedule=schedule,
              start_date=pendulum.datetime(2023, 1, 1, tz=timezone),
              catchup=False)

    # Build data processing pipeline
    with dag:

        create_process_id_task = create_process_id()

        start_process_task = AddProcessStatusOperator(
            task_id="start_process",
            model_factory_wrapper=model_factory_wrapper,
            source_code=source_code.value,
            process_id=PROCESS_ID,
            process_status=ProcessStatus.NEW,
            retries=0)

        finish_process_task = AddProcessStatusOperator(
            task_id="finish_process",
            model_factory_wrapper=model_factory_wrapper,
            source_code=source_code.value,
            process_id=PROCESS_ID,
            process_status=ProcessStatus.LOADED,
            retries=0)

        dataflow_job_task = create_dataflow_job_task(dataflow_config=dataflow_config,
                                                     dataflow_template=dataflow_template)

        bq_load_task = create_bq_load_task(bq_load_config=bq_load_config,
                                           source_bucket=dataflow_template["output_bucket"],
                                           source_folder=data_output_folder)

        create_process_id_task >> start_process_task >> dataflow_job_task >> bq_load_task >> finish_process_task

    return dag


# Generate DAGs for pipelines
for source_name, pipeline_config in config["pipelines"].items():
    source_code = SourceCodes[source_name]

    dag_id = f"{source_code.name}_dag"

    globals()[dag_id] = create_dag(dag_id=dag_id,
                                   source_code=source_code,
                                   pipeline_config=pipeline_config)