import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from faker import Faker


parser = argparse.ArgumentParser()
parser.add_argument('--bucket_name', type=str, required=True)
parser.add_argument('--s3_folder', type=str, default='emr_development/users')
parser.add_argument('--num_of_iterations', type=int, default=1000)
args = parser.parse_args()

BUCKET_NAME = args.bucket_name
S3_FOLDER = args.s3_folder
NUM_OF_ITERATIONS = args.num_of_iterations


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
