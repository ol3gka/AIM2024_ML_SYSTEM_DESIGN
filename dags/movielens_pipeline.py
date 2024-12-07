from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import zipfile
import os
import requests
import pandas as pd
import logging
logging.basicConfig(level=logging.DEBUG)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import FloatType

from minio import Minio
from io import BytesIO
import sys
sys.path.append('.')

# Логин, пароль - "minioadmin" - можно положить в переменные окружения Airflow
# то же касается название бакета movielens. Также разумно хранить .env файл а не харкодить названия
# но для учебных целей - ОК
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "movielens"


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'movielens_pipeline',
    default_args=default_args,
    description='Pipeline for MovieLens dataset',
    schedule_interval=None,
)

def download_and_extract_data():
    url = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
    response = requests.get(url)
    with open("ml-latest-small.zip", "wb") as file:
        file.write(response.content)
    with zipfile.ZipFile("ml-latest-small.zip", 'r') as zip_ref:
        zip_ref.extractall("data")
    logging.info('Данные скачены')


def split_data():
    ratings = pd.read_csv("data/ml-latest-small/ratings.csv")
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    train = ratings[ratings['timestamp'] < '2015-01-01']
    test = ratings[ratings['timestamp'] >= '2015-01-01']
    logging.info(f'train: {len(train)}, test: {len(test)}')
    train.to_csv("/shared_data/train_ratings.csv", index=False)
    test.to_csv("/shared_data/test_ratings.csv", index=False)
    logging.info('Данные разделены на train и test')


def upload_to_minio():
    client = Minio("minio:9000", access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    found = client.bucket_exists(BUCKET_NAME)
    if not found:
        client.make_bucket(BUCKET_NAME)
        logging.info('Бакет movielens создан')
    else:
        logging.info('Bucket movielens already exists!')

    train = pd.read_csv("/shared_data/train_ratings.csv")
    test = pd.read_csv("/shared_data/test_ratings.csv")

    csv_train_bytes  = train.to_csv().encode('utf-8')
    csv_test_bytes = test.to_csv().encode('utf-8')

    client.put_object(
        BUCKET_NAME,
        'train_ratings.csv',
        data=BytesIO(csv_train_bytes),
        length=len(csv_train_bytes),
        content_type='application/csv')

    client.put_object(
        BUCKET_NAME,
        'test_ratings.csv',
        data=BytesIO(csv_test_bytes),
        length=len(csv_test_bytes),
        content_type='application/csv')

    logging.info('myminio upload успешно')

def apply_model():
    spark = SparkSession.builder.appName("MovieLensALS").master("spark://spark-master:7077").getOrCreate()
    logging.info('Сессия создана')
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.acces.key", os.getenv("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY", MINIO_SECRET_KEY))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", os.getenv("ENDPOINT", 'http://minio-server:9000'))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")
    spark.sparkContext.setLogLevel("WARN")

    ratings_schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", IntegerType(), True)
    ])
    train_data = spark.read.csv(f"s3a://{BUCKET_NAME}/train_ratings.csv", header=True, schema=ratings_schema)
    test_data = spark.read.csv(f"s3a://{BUCKET_NAME}/test_ratings.csv", header=True, schema=ratings_schema)
    train_data.show(20, False)
    als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
    logging.info('Обучение начато')
    model = als.fit(train_data)
    logging.info('model fit успешно')
    try:
        model.save("/shared_data/als_model")
    except:
        logging.info('Не удалось созранить модель')
    predictions = model.transform(test_data)
    logging.info('model predict успешно')
    predictions.write.format("csv").mode("overwride").save(f"s3a://{BUCKET_NAME}/predictions")
    pred_df = spark.read.csv(f"s3a://{BUCKET_NAME}/predictions")
    pred_df.show(20, False)
    spark.stop()
    logging.info('Расчет закончен успешно')


download_data = PythonOperator(
    task_id='download_and_extract_data',
    python_callable=download_and_extract_data,
    dag=dag,
)

split_data = PythonOperator(
    task_id='split_data',
    python_callable=split_data,
    dag=dag,
)

upload_data = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_to_minio,
    dag=dag,
)

apply_model = PythonOperator(
    task_id='apply_model',
    python_callable=apply_model,
    dag=dag,
)

download_data >> split_data >> upload_data >> apply_model