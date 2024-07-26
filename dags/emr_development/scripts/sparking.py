from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

BUCKET_NAME = 'panzer-development'

spark = SparkSession.builder \
    .appName('Example Spark Script') \
    .getOrCreate()

schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('city', StringType(), True)
])

data = [
    ('Alice', 29, 'New York'),
    ('Bob', 35, 'San Francisco'),
    ('Cathy', 19, 'Los Angeles'),
    ('David', 40, 'Chicago'),
    ('Eva', 22, 'Seattle')
]

df = spark.createDataFrame(data, schema)

df.write.mode('overwrite').parquet(f's3://{BUCKET_NAME}/spark_example/')
