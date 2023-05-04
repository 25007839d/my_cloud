
from airflow.models import Variable
from _datetime import datetime
current_time = datetime.now()


current_time1 = current_time.strftime("%d-%m-%Y")
Variable.set("file_path", current_time1)  # add path to variable env
