
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import *
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


#======================================================================================================================================
'''SCD task 1'''

import re
import os
import sys
import logging

# create file record logger program
logger = logging.getLogger('simple_example')
logger.setLevel(logging.DEBUG)
ch = logging.FileHandler(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\SCD PROJECT\Dushyant\SCD.log")
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
#===================================================================================
# file selection
a=os.listdir(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\SCD PROJECT\Dushyant\Raw_data_file")
print(a)
rdd=sc.parallelize(a)
print(rdd.collect())
#
def date_extract (ele):
    x = re.search("\d{4}_\d{2}_\d{2}",ele)
    return x[0]

rdd1=rdd.map(lambda x: date_extract(x))
print(rdd1.collect())
date_collect = (rdd1.sortBy(lambda x:x,ascending=False).collect()[0])
print(date_collect)
latest_file = rdd.filter(lambda x: date_extract(x) == date_collect).collect()[0]
print(latest_file)
dir=r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\SCD PROJECT\Dushyant\Raw_data_file"
#======================================================================================
#log file read and filter condition apply

j=open(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\SCD PROJECT\Dushyant\SCD.log")
x=j.read()

if  latest_file not in x:
    link= dir+"\\" + latest_file
    print(link)
    df_readfile= spark.read.option("header", True).csv(link)
    df_readfile.show()


    # from pyspark.sql.window import Window
    # from pyspark.sql.functions import row_number,col
    df_readfile1=df_readfile.withColumn("date",lit(date_collect)).orderBy('date')
    df_readfile2=df_readfile1.repartition(1)
    df_readfile2.show()

    #file write at 1st Target

    df_readfile2.write.mode('append').option('header',True).csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\SCD PROJECT\Dushyant\1st_target")

    #successfull file write log create
    logger.info(latest_file)

    #read target 1st
    df_readtarget= spark.read.option("header", True).csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\SCD PROJECT\Dushyant\1st_target")
    df_readtarget.show(10000)

#SCD1 program
    from pyspark.sql.window import *
    wind=Window.partitionBy('dept_id').orderBy('date')
    df_readtarget1=df_readtarget.withColumn("row_num",row_number().over(wind)).where(col('row_num')==1)
    df_writefinal=df_readtarget1.repartition(1)
    df_writefinal.show()
    # '''write'''
    df_writefinal.write.mode('overwrite').option('header',True).csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\SCD PROJECT\Dushyant\Final_target _1")

#SCD2 program
    from pyspark.sql.window import *
    wind=Window.partitionBy('dept_id').orderBy('date')
    df_readtarget1=df_readtarget.withColumn("row_num",row_number().over(wind))
    df_writefinal=df_readtarget1.repartition(1)
    df_writefinal.show()
    # '''write'''
    df_writefinal.write.mode('overwrite').option('header',True).csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\SCD PROJECT\Dushyant\Final_target _2")

#SCD3 program
    from pyspark.sql.window import *
    wind=Window.partitionBy('dept_id').orderBy('date')
    df_readtarget1=df_readtarget.withColumn("row_num",row_number().over(wind)).where(col('row_num')<3)
    df_writefinal=df_readtarget1.repartition(1)
    df_writefinal.show()
    # '''write'''
    df_writefinal.write.mode('overwrite').option('header',True).csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\SCD PROJECT\Dushyant\Final_target _3")

else:
    sys.exit()



# '''write 1str Target'''
#
# '''window + with col'''
# wind=Window.partitionBy('dept_id').orderBy('date')
# udd2=udd1.withColumn("row_num",row_number().over(wind)).where(col('row_num')==1)
# udd3=udd2.repartition(1)
# udd3.show()
# '''write'''
# udd3.write.mode('overwrite').option('header',True).csv(r"C:\Users\RITU\OneDrive\Desktop\Brainwork\Cloud\project\Dushyant\Final_target")
#


