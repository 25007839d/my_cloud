
import findspark
from google.cloud.storage import blob
from google.cloud import bigquery
findspark.init()
from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark import SparkFiles
from pyspark.conf import SparkConf
import os
os.environ["GCLOUD_PROJECT"]="dushyant-373205"

# as connector
import os
directory_path = os.getcwd()
#
SERVICE_ACCOUNT_FILE =os.path.join(directory_path,'cre.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE


# spark = SparkSession.builder \
#   .appName('Optimize BigQuery Storage') \
#   .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.15.1-beta') \
#   .config("spark.sql.broadcastTimeout", "36000") \
#    .config("credentialsFile", SERVICE_ACCOUNT_FILE)\
#     .config("spark.jars", "gs://hadoop-lib/gcs/gcs-connector-hadoop2-2.1.1.jar") \
#     .config("spark.submit.pyFiles", "gs://spark-lib/bigquery/spark-bigquery-support-0.26.0.zip") \
#     .config("spark.files", "gs://spark-lib/bigquery/spark-bigquery-support-0.26.0.zip") \
#     .config(conf=SparkConf().setAll([('viewsEnabled', 'true'), ('materializationDataset', 'name')])) \
#     .enableHiveSupport() \
#     .getOrCreate()
#
# spark =SparkSession.builder \
#     .master('yarn') \
#     .appName('spark-read-from-bigquery') \
#     .config('spark.jars.packages','com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-1.9.5,com.google.guava:guava:r05') \
#     .config('spark.jars','guava-11.0.1.jar,gcsio-1.9.0-javadoc.jar') \
#     .getOrCreate()


from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "dush_01"
spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery.
words = spark.read.format('bigquery') \
  .option('table', 'dushyant-373205:name.bbb') \
  .load()




        # df.write.format('bigquery') \
        # .option('table', 'ritu-351906:bwt_session.try5') \
        # .save()
# df = spark.read \
#   .format("bigquery") \
#   .option('credentialsFile',SERVICE_ACCOUNT_FILE)\
#   .option('parentProject','dushyant-373205')\
#   .option('table',"name.bbb")\
#   .load()


words.show()

        # df.write.option("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"). \
        #         option("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"). \
        #         option("google.cloud.auth.service.account.enable", "true"). \
        #         option("google.cloud.auth.service.account.json.keyfile",
        #                r"C:\Users\Dell\PycharmProjects\spark\etl_project\ddl\formal-purpose-352005-d58e2dcced94.json"). \
        #         mode("overwrite"). \
        #         csv('gs://etl-d-r/{}/file.csv'.format(table), header=True)