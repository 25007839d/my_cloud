from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, asc,row_number,lit
from pyspark.sql.window import Window

spark = SparkSession.builder\
    .master('yarn')\
  .appName('BigNumeric')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar')\
  .config('spark.submit.pyFiles', 'gs://spark-lib/bigquery/spark-bigquery-support-0.27.0.zip')\
  .config('spark.files', 'gs://spark-lib/bigquery/spark-bigquery-support-0.27.0.zip')\
  .getOrCreate()

# sqlContext.setConf("spark.sql.shuffle.partitions", "6") # for local cluster
# sqlContext.setConf("spark.sql.files.maxPartitionBytes","25600") #for gcs cloud server

# Cloud storage connector is required to read from GCS in Spark.
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

spark.conf.set('temporaryGcsBucket','demo_011') # for bq load temp bucket

# df=spark.read.csv(r'gs://ritu_bucket/customers.csv',header=True)

# windowSpec  = Window.partitionBy("country").orderBy("country")

# df1=df.withColumn("row_number",row_number().over(windowSpec))
# df.show()
# print('print df rdd get num part')
# print(df.rdd.getNumPartitions())
# print('print partitionby rdd get num part')
# print(df.rdd.getNumPartitions())
#
# print('print df part count')
# df \
#         .withColumn("partitionId", spark_partition_id()) \
#         .groupBy("partitionId") \
#         .count() \
#         .orderBy(asc("count")) \
#         .show()
# print('df1 repartition(2)')
# df1=df.repartition(2) # data well manage after this
# df2=df1.join(df, df.Country==df1.Country, 'inner')
# df3=df2.repartition(8)
# df4 =df3.withColumn('new',lit('1'))
#
# print('df1 part count')
# df2 \
#         .withColumn("partitionId", spark_partition_id()) \
#         .groupBy("partitionId") \
#         .count() \
#         .orderBy(asc("count")) \
#         .show()# no impact of partitionBy clause on data partition
# print('df2 part count')
#
#
# df4 \
#         .withColumn("partitionId", spark_partition_id()) \
#         .groupBy("partitionId") \
#         .count() \
#         .orderBy(asc("count")) \
#         .show()
# df.write.format('bigquery').option('table','spatial-shore-360518.gcs_to_bq_df.rdx').mode('append').save()
# df.write \
#   .format("bigquery") \
#   .option("temporaryGcsBucket","ritu_bucket") \
#   .save("gcs_to_bq_df.rdx")
# print('bq load succesfully')

words = spark.read.format('bigquery') \
        .option('table', 'dushyant-373205:dcddd.hfjhk') \
        .load()
words.show()