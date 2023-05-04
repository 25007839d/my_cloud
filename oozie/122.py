# <action name="touch_success" retry-max="3" retry-interval="1">
#         <fs>
#             <delete path="$(root__context_hive_ddl_success_file_root)/${nominal_time}/${DATASET}"/>
#             <mkdir path="$(root__context_hive_ddl_success_file_root)/${nominal_time}/${DATASET}"/>
#             <touchz path="$(root__context_hive_ddl_success_file_root)/${nominal_time}/${DATASET}/_SUCCESS"/>
#             <chmod path="$(root__context_hive_ddl_success_file_root)/${nominal_time}/${DATASET}/_SUCCESS" permis
# <chmod path="$(root__context_hive_ddl_success_file_root)/${nominal_time}/${DATASET}/_SUCCESS" permissions="640" dir-files="true"/>
#         </fs>
#         <ok to="end"/>
#         <error to="fail"/>
#     </action>


from airflow import DAG,settings
import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_delete_operator import GCSDeleteObjectsOperator
import os
from google.cloud import storage
bucket_n = "demo_0111"
from pathlib import Path
import os, sys, stat
# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

def view (path):
    print(path)
def delete_bucket(bucket_name):
    """Deletes a bucket. The bucket must be empty."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete()

    print(f"Bucket {bucket.name} deleted")

dag_folder_path = os.path.dirname(settings.DAGS_FOLDER)
conf_file_path = os.path.join(dag_folder_path, 'dags')

current_dir = os.getcwd()


def dag_path():
    file = open(current_dir + '/demo.txt','a')
    file.close()


    # Assuming /tmp/foo.txt exists, Set a file execute by the group.

    print("PPPPPPPPPPPPPPPPPPPPPPPPPP", file, "uuuuuuuuuuuuuuuuuuuuuuuuuu")

def chmod():
    os.chmod('/home/airflow/demo.txt', stat.S_IXGRP)
    print("**************ssuming /tmp/foo.txt exists, Set a file execute by the group.",os.listdir("/home/airflow/"))

default_arg = {
                'owner':"oozie",
                'start_date':datetime.datetime(2022,1,18),
                'retry': 3,
                'retry_delay':timedelta(minutes=1),
                'catchup':False,
                'schedule_interval':"*/20 * * * *"


                }
with DAG(dag_id="oozie_dag",default_args=default_arg) as dag:

    # delete = BashOperator(
    #                     task_id="delete_dir",
    #                     bash_command="gsutil mb gs://demo_031111/"
    #
    # )
    #
    #
    # create = BashOperator(
    #                     task_id="create_bucket",
    #                     bash_command="gsutil mk gs://demo_01111"
    #
    # )
    # delete = PythonOperator(
    #                     task_id="delete_bkt",
    #                     python_callable=view(BASE_DIR),
    #                     dag = dag
    #
    #
    #                             )
    task1 = PythonOperator(
        task_id='dag_path',
        python_callable=dag_path,
        dag=dag
    )
    # task2 = PythonOperator(
    #     task_id='conf_file',
    #     python_callable=view(BASE_DIR),
    #     dag=dag)
    create = BashOperator(
                        task_id="make_dir",
                        bash_command="gsutil cp /home/airflow/demo.txt gs://demo_02111/script/"
    )
    change = PythonOperator(
        task_id='cdmod',
        python_callable=chmod,
        dag=dag
    )
    chmode = BashOperator(
        task_id="chmod_bash",
        bash_command="chmod 640 /home/airflow/demo.txt ")
    chmode_read = BashOperator(
        task_id="chmod_ls",
        bash_command="cd /home/airflow ls -ltrha")
