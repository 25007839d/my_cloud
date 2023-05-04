# This is a sample Python script.
import os
# import re
import json
import findspark
# findspark.init()
# import json
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType,StructField,IntegerType,DateType,StringType
# # c='ds$f'
# # d=['fdsdf','fd43#']
# # a= [re.sub('[\W]','xx',c) for c in d]
# # print(a)
# column = []
# with open(r"D:\Brainwork\Cloud\PCM PROJECT\schema\schema.json",'r') as file:
#     j_file = json.load(file)
#
#     print(j_file)
#     for i in j_file:
#         column.append(i['name'])
#
# # print(column)
#
# schema=StructType([])
#
# data_types={
#         "integer":IntegerType(),
#         "string":StringType(),
#         "date":DateType(),
#         "integer":IntegerType()}
#
# for i in j_file:
#     schema.add(i["name"], data_types.get(i["type"]))
# # print(schema)
#
# l =[1,3,4,5]
#
# a= list(map(lambda x:x**x,l ))
# print(a)
import os

from datetime import datetime,timedelta


current_dir = os.getcwd()

os.path.join(current_dir,"self")

print(os.path.join(current_dir,"self"))