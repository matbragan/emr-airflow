from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from faker import Faker

BUCKET_NAME = 'panzer-development'
S3_FOLDER = 'emr_development/users'
NUM_OF_ITERATIONS = 1000

fake = Faker('pt_BR')

spark = SparkSession.builder \
    .appName('Sparking Users') \
    .getOrCreate()

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('state', StringType(), True),
    StructField('birthday', DateType(), True)
])

data = [(i, fake.name(), fake.state(), fake.date_of_birth()) for i in range(1, NUM_OF_ITERATIONS)]

df = spark.createDataFrame(data, schema)

df.write.mode('overwrite').parquet(f's3://{BUCKET_NAME}/{S3_FOLDER}/')
