import random
import datetime

import numpy as np
from pyspark.sql import SparkSession

BUCKET_NAME = 'panzer-development'
S3_FOLDER = 'emr_development/sales'
NUM_OF_ITERATIONS = 1000

spark = SparkSession.builder \
    .appName('Sparking Sales') \
    .getOrCreate()

data = []
for num in range(NUM_OF_ITERATIONS):
    new_sale = {
        'saleID': num,
        'userID': random.randint(1, NUM_OF_ITERATIONS),
        'productID': np.random.randint(10000, size=random.randint(1, 5)).tolist(),
        'paymentMthd': random.choice(['Credit card', 'Debit card', 'Digital wallet', 'Cash on delivery', 'Cryptocurrency']),
        'totalAmt': round(random.random() * 5000, 2),
        'invoiceTime': datetime.datetime.now().isoformat()
    }

    data.append(new_sale)

df = spark.createDataFrame(data)

df.write.mode('overwrite').parquet(f's3://{BUCKET_NAME}/{S3_FOLDER}/')
