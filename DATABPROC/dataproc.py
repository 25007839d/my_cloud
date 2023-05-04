from pyspark.sql import SparkSession
from airflow import models
from airflow.providers.google.cloud.operators.dataflow import (DataprocSubmitJobOperator,)



if __name__ == "__main__" :

    spark = SparkSession.builder \
             .config('spark.jars.packages',
                 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-1.9.5,com.google.guava:guava:r05') \
         .master('local[*]').appName('spark-bigquery-demo').getOrCreate()


    # spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",f"gs://pcm_dev-ingestion1/ritu_gopal/code/ritu-351906-6a298c30c864.json")


    simpleData="python /home/airflow/gcs/dags/department_2022_05_21.csv"



    df = spark.read.csv(path=simpleData, header=True)

    df.printSchema()
    df.show(truncate=False)

    buckt= "pcm_dev-ingestion1"
    spark.conf.set('temporaryGcsBucket',buckt)

    #
    # df.write.format('bigquery').option('table','ritu_dush01.ram').mode('append').save()
    # print("write_data")

    df.write.format('bigquery') \
        .option('table', 'ritu_dush01.ram78') \
        .save()


    df.write.mode('append').option('header',True).csv(r"python /home/airflow/gcs/dagsgood_data")
    print("load_good data on gcs")
