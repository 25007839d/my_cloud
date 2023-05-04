
import findspark
from google.cloud.storage import blob
from google.cloud import bigquery
findspark.init()
from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark import SparkFiles
from pyspark.conf import SparkConf

from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()


bucket = "dush_01"
spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery.
words = spark.read.format('bigquery') \
  .option('table', 'dushyant-373205:name.bbb') \
  .load()




words.show()
