from google.cloud import storage
import bigquery as bigquery
import findspark
import os
from google.cloud.bigquery import Client
from google.cloud.storage import blob
from pyspark import StorageLevel

findspark.init()
findspark.init()
from pyspark.sql import SparkSession
from pyspark import *
from google.cloud import storage
import os
os.environ["GCLOUD_PROJECT"]="ritu-351906"

# storage_clint = storage.Client()
# bucket = storage_clint.bucket('pcm_dev-ingestion2')
# bucket.storage_class = "STANDARD"
# nw_bucket = storage_clint.create_bucket(bucket,location='us')
# print("bucket_name"+str(nw_bucket))

storage_client = storage.Client()
bucket = storage_client.bucket("pcm_dev-ingestion3")
bucket.storage_class = "COLDLINE"
new_bucket = storage_client.create_bucket(bucket, location="us")

print (new_bucket.location,new_bucket.storage_class)
spark = SparkSession.builder\
        .master("local[2]")\
        .appName("SparkByExamples.com")\
        .getOrCreate()



obj_clint = storage.Client.from_service_account_json(r"C:\Big Data\ritu-351906-c603422b2e4b.json")
#

spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",r"C:\Big Data\charming-shield-350913-2a1c8524ef6c.json")

path = f"gs://pcm_dev-ingestion/data/my_sql/data/*"
sdf= spark.read.csv(path, header=True)
sdf.show()

'''delete bucket'''
#
#
storage_client = storage.Client()

bucket = storage_client.get_bucket("dush-python-bucket")
bucket.delete()

print(f"Bucket {bucket.name} deleted")

'''upload the file'''
storage_client = storage.Client()
bucket = storage_client.bucket("pcm_dev-ingestion")
blob = bucket.blob('cloud assingment.txt')
#
blob.upload_from_filename(r"C:\Users\RITU\OneDrive\Desktop\Brainwork\Cloud\Assingment of cloud.txt")

print(blob.upload_from_filename)


'''download the file'''


blob.download_to_filename(f"gs://pcm_dev-ingestion1/data/my_sql/data.csv")
#

