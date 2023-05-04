from airflow.api.common.experimental.get_task_instance import get_task_instance

def write_entry(logger_name):
    """Writes log entries to the given logger."""
    logging_client = logging.Client()

    # This log can be found in the Cloud Logging console under 'Custom Logs'.
    logger = logging_client.logger(logger_name)

    # Make a simple text log
    logger.log_text("Hello, world!")

    # Simple text log with severity.
    logger.log_text("Goodbye, world!", severity="ERROR")

    # Struct log. The struct can be any JSON-serializable dictionary.
    logger.log_struct(
        {
            "name": "King Arthur",
            "quest": "Find the Holy Grail",
            "favorite_color": "Blue",
        }
    )

    print("Wrote logs to {}.".format(logger.name))

# =======================================================
def list_entries(logger_name):
    """Lists the most recent entries for a given logger."""
    logging_client = logging.Client()
    logger = logging_client.logger(logger_name)
    print(logger.path)

    print("Listing entries for logger {}:".format(logger.name))

    for entry in logger.list_entries():
        timestamp = entry.timestamp.isoformat()

        print("* {}: {}:{}".format(timestamp, entry.payload, entry[0]))


new = list_entries('my-log')

def check_status(**kwargs):
    last_n_days = 10
    for n in range(0,last_n_days):
        date = kwargs['execution_date']- timedelta(n)
        ti = TaskInstance(*my_task*, date) #my_task is the task you defined within the DAG rather than the task_id (as in the example below: check_success_task rather than 'check_success_days_before')
        state = ti.current_state()
        if state != 'success':

def get_dag_state(execution_date, **kwargs):
                ti = get_task_instance('dag_id', 'task_id', execution_date)
                task_status = ti.current_state()
                return task_status
dag_status = BranchPythonOperator(
                task_id='dag_status',
                python_callable=get_dag_state,
                dag=dag
            )