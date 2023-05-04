"""
DAGs for ETL common pipeline my new cloud_1 branch
test for master
"""
import datetime

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator


def create_dag():

    # dataflow_config = pipeline_config["dataflow"]
    #
    # bq_load_config = pipeline_config["bq_load"]
    # bq_load_config["schema_bucket"] = f"bq-schemas-{PROJECT_ID}"
    #
    # data_output_folder = f"{source_code.value}/{RUN_DATE}/{PROCESS_ID}"

    # dataflow_template = render(
        # search_path=template_search_path,
        # template_name="common_pipeline_dataflow.yaml",
        # data={
        #     "PROCESS_ID": PROCESS_ID,
        #     "SOURCE_CODE": source_code.value,
        #     "RUN_DATE": RUN_DATE,
        #     "PROJECT_ID": PROJECT_ID,
        #     "LOCATION": LOCATION,
        #     "NETWORK": NETWORK,
        #     "SUBNETWORK": SUBNETWORK,
        #     "CONFIG_NAME": dataflow_config["config_name"],
        #     "LOG_LEVEL": dataflow_config.get("log_level", "INFO"),
        # }
    # )



    dag = DAG(dag_id="test_dag",
              schedule_interval="* */1 * * *",
              start_date= datetime.datetime(2022, 9, 14),
              catchup=False)

    # Build data processing pipeline
    with dag:



        start_process_task =DummyOperator(
            task_id= "start"
        )

        finish_process_task = DummyOperator(
            task_id= "end"
        )

        start_process_task >>  finish_process_task

    return dag


# Generate DAGs for pipelines
for i in range(1,4):


    dag_id = "test_dag"

    globals()[dag_id] = create_dag()
