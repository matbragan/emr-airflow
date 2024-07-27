from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from faker import Faker

BUCKET_NAME = 'panzer-development'
S3_FOLDER = 'emr_development/faker_people'

spark = SparkSession.builder \
    .appName('Faker People') \
    .getOrCreate()

fake = Faker('pt_BR')

schema = StructType([
    StructField('name', StringType(), True),
    StructField('state', StringType(), True),
    StructField('birthday', DateType(), True)
])


data = [(fake.name(), fake.state(), fake.birthday()) for _ in range(1000)]

df = spark.createDataFrame(data, schema)

df.write.mode('overwrite').parquet(f's3://{BUCKET_NAME}/{S3_FOLDER}/')
