from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkFiles
from pyspark.conf import SparkConf
import os
os.environ["GCLOUD_PROJECT"]="dushyant-373205"
SCOPES = ['https://www.googleapis.com/auth/sqlservice.admin']
import os
directory_path = os.getcwd()
#
SERVICE_ACCOUNT_FILE =os.path.join(directory_path,'cre.json')




spark = SparkSession \
    .builder \
    .appName("nrb") \
    .config("spark.sql.broadcastTimeout", "36000") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .config("spark.submit.pyFiles", "gs://spark-lib/bigquery/spark-bigquery-support-0.26.0.zip") \
    .config("spark.files", "gs://spark-lib/bigquery/spark-bigquery-support-0.26.0.zip") \
    .config(conf=SparkConf().setAll([('viewsEnabled', 'true'), ('materializationDataset', 'name')])) \
    .enableHiveSupport() \
    .getOrCreate()
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
access_token=credentials.get_access_token()
obj_clint = storage.Client.from_service_account_json(f"gs://dush_01/dushyant-373205-339efc449b7d.json")
#

spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",f"gs://dush_01/dushyant-373205-339efc449b7d.json")


spark.conf.set('temporaryGcsBucket','dush_01') # for bq load temp bucket
# SERVICE_ACCOUNT_FILE = {
#   "type": "service_account",
#   "project_id": "dushyant-373205",
#   "private_key_id": "339efc449b7d36ed8285396d15565d72cd94e1be",
#   "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDkZ8Nbxni9zFbV\nI8eK+Bf0JmOiGh5cXG1dzk+VzXxvdfyNqVLyz5NcJ7WSyvWbnv8RNSvBHjj38FiH\nfEH8k9xdH24UA7/F0+IQL1UaX/Dou0QqqIqwXYxUip7z0z8UELREGh7cEfGqWJy2\nFYQjYHTNbYuR1mh6bIBueHkMpbzLFW/50Yur97oQ3r7V1jkCz2CEblrBeP7lGUFW\nvF1Ik3sTpKIwIMHfbMjrN3kWild1w9TNZJSm1ESiI82ksB3L2RI4p16XNeOzvdKL\n2Jk/Sw1v9Udi+txbkKwjai3Eao3CTUz6HUuDoJgMSbHyYA2/ha7Fuc/RE6DdYWjp\nKlp0rkWXAgMBAAECggEAIR4UevWjoy2CN6RY9gL0AjAgySvNVCoIp43XB0sErGqo\npGe1QcSsQrP1RWQNSbO7myShA/7cVsLwKzDupm9QB+abQOasQ0RuJleemNTpau1g\nPGk0ts7Rnp583rC2GDQ11xwMdm9ZoY/4pQQopTKCrRihoJM8keeylZH9R0Kuxak/\nYqelOFWt1LFhk6YlF9mvOgHFFv3dq4mjmlAqQXyDVXvRMMp93pL89z94PYXpoKP4\n5SPcRfXpHAzn2GGUvVHWw29QGsqhKHJk4whRueQNqksSfPCANJmQvTRM0WW609dD\n/2w3UoquQp49bwhDPMS+KF9ET0G8GiZdy4YxkoYMVQKBgQD8vFvLADW0FDv/BJJ/\nNkkHZ1KQPQpOufyor7lazX18d/mzJAbeI+HF4hZ6rXv3ih10866EwiwR/tpqB7e/\nP4jWwZsGCMHZv88Z8ujrBztJjAY9gfgAG+om9td0FmgkO/fRU3Z1vd9hoMdGR26q\nmEjNAv1Apd4XnfOQCEhVWEFR9QKBgQDnWvVwf0g/tbkkksHTooEDCMux7WEiAdSz\nzznz2SCPW7iMIhRZWiF8V2RinWyG42BlZai3K5KFv+kKPpmyq/rGhfwLxhsHkQv7\nkqrGGIwiLAhy/HM84XW/yfULEw+ltRmIIfqBRHD57lTZhdq1tf6xNhHmsnYrbm1g\nkOyopWnl2wKBgCBGKW6t8y2w28yAF/kYxJCmeBulP9i31XTxI7ldvmWTqJZgxmO9\nr0omyfoWh1sgDwztRV78sMYHhnq9y5Anm+DYzmQh4CdfYCIPLAE/ZinJMM7P9wyE\nhA0/vlm7tsbRxZ68iscUXR9JeckWvCIa2kkb1Z1Y0Riw+fZtqFcFsym1AoGAOqvK\nipj8zYtcRHYjSXRwm0Bjx9+XPnSQaHkpDToHE9QC3SbXWkGNNGdFQe0CVMLc81nw\ni2M1aU3d34c4myMaGbZo0OOQfz69dzMes9YN06yvB9oVr17N6bFhv0k6Mp/WtbtI\nN/gPXpKeWfukF2jsCizYJVUkuqbqfg7CtZIdZPkCgYAnB6w4tNmJAyB/6ttWzEba\nZ0o32+h+KbdtZKQo0AxSHR2kW/Kni61DGrh6dpZZDPWdC+C1vXAphdfET/b+skfO\nTYKGupvF1QSxNvXb4y7M/WNnXSY6ug73QcfRNioVQ6US5fLmQ+ClOGsyexp7t9e8\nd3yc2NTNwQPMBc5EBkm/pg==\n-----END PRIVATE KEY-----\n",
#   "client_email": "bigquery-read@dushyant-373205.iam.gserviceaccount.com",
#   "client_id": "110595284122092484550",
#   "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#   "token_uri": "https://oauth2.googleapis.com/token",
#   "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#   "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bigquery-read%40dushyant-373205.iam.gserviceaccount.com"
# }


df = spark.read \
  .format("bigquery") \
  .option('credentialsFile',SERVICE_ACCOUNT_FILE)\
  .option('parentProject','dushyant-373205')\
  .option('table',"name.bbb")\
  .load()


df.show()