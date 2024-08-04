import random
import datetime
import argparse

import numpy as np
from pyspark.sql import SparkSession


parser = argparse.ArgumentParser()
parser.add_argument('--bucket_name', type=str, required=True)
parser.add_argument('--s3_folder', type=str, default='emr_development/sales')
parser.add_argument('--num_of_iterations', type=int, default=1000)
args = parser.parse_args()

BUCKET_NAME = args.bucket_name
S3_FOLDER = args.s3_folder
NUM_OF_ITERATIONS = args.num_of_iterations


spark = SparkSession.builder \
    .appName('Sparking Sales') \
    .getOrCreate()

data = []
for num in range(1, NUM_OF_ITERATIONS):
    new_sale = {
        'id': num,
        'userId': random.randint(1, NUM_OF_ITERATIONS),
        'productId': np.random.randint(10000, size=random.randint(1, 5)).tolist(),
        'paymentMthd': random.choice(['Credit card', 'Debit card', 'Digital wallet', 'Cash on delivery', 'Cryptocurrency']),
        'totalAmt': round(random.random() * 5000, 2),
        'invoiceTime': datetime.datetime.now().isoformat()
    }

    data.append(new_sale)

df = spark.createDataFrame(data)

df.write.mode('overwrite').parquet(f's3://{BUCKET_NAME}/{S3_FOLDER}/')
