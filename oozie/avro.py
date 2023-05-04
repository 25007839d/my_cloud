#avro-example.py
import findspark
findspark.init()
from pyspark.sql import SparkSession

appName = "PySpark Example - Read and Write Avro"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# List
data = [{
    'col1': 'Category A',
    'col2': 100
}, {
    'col1': 'Category B',
    'col2': 200
}, {
    'col1': 'Category C',
    'col2': 300
}]

df = spark.createDataFrame(data)
df.show()
df.write.format("avro").mode("overwrite").save("gs://demo_011/data/avro/a.avro")


# Save as Avro
# df.write.format('avro').mode('overwrite').save(
#     "fcf.avro")