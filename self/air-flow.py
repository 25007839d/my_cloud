
import os
import sys
# get_trans_dt ="fd"
# outputFile='dd'
# print(' '.join(["hadoop", "fs", "-test", "-e"]))
#
# print(f"ritu {get_trans_dt} dushyant")

# a= str(['d','f'])
# d={}
# d[a]=2
# print(d)
# import time
# from datetime import datetime
# timestamp = time.time()
# print(timestamp)
# EPOCH = datetime.utcfromtimestamp(0)
# print(EPOCH)

import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('dsa').getOrCreate()

df = spark.read.csv(r"D:\Brainwork\Cloud\PCM PROJECT\rawdata/data.csv")
df.show()

















