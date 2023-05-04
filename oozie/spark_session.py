from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.conf import SparkConf
import os
os.environ["GCLOUD_PROJECT"]="dushyant-373205"
# spark = SparkSession \
#     .builder \
#     .appName("nrb") \
#     .master('yarn')\
#     .config("spark.sql.broadcastTimeout", "36000") \
#     .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
#     .config("spark.submit.pyFiles", "gs://spark-lib/bigquery/spark-bigquery-support-0.26.0.zip") \
#     .config("spark.files", "gs://spark-lib/bigquery/spark-bigquery-support-0.26.0.zip") \
#     .config(conf=SparkConf().setAll([('viewsEnabled', 'true'), ('materializationDataset', 'name')])) \
#     .enableHiveSupport() \
#     .getOrCreate()

#
#
spark = SparkSession.builder\
    .master('yarn')\
  .appName('BigNumeric')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.28.0.jar')\
  .config('spark.submit.pyFiles', 'gs://spark-lib/bigquery/spark-bigquery-support-0.27.0.zip')\
  .config('spark.files', 'gs://spark-lib/bigquery/spark-bigquery-support-0.27.0.zip')\
  .config(conf=SparkConf().setAll([('viewsEnabled', 'true'), ('materializationDataset', 'name')])) \
  .enableHiveSupport() \
  .getOrCreate()

conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

spark.conf.set('temporaryGcsBucket','dush_01') # for bq load temp bucket

obj_clint = storage.Client.from_service_account_json("gs://demo_011/dushyant-373205-62832aac51fd.json")
#

spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","gs://demo_011/dushyant-373205-62832aac51fd.json")

# df = spark.read \
#   .format("bigquery") \
#   .load("vz-it-pr-gudv-dtwndo-0.aid_dtwin_ran_core_tbls_v.dim_inventory_enodeb_site_map_norm_v0")
df = spark.read.format('bigquery') \
        .load('table', 'dushyant-373205.name.bbb')

# df = spark.read.csv(r'gs://demo_011/data.csv')
df.show()
# df.write.format('bigquery').option('table','dushyant-373205.name.bbb').mode("overwrite").save()
