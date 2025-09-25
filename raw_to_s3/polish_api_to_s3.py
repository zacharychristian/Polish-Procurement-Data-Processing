from pyspark.sql import SparkSession
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import findspark
import json
import os

#Set timezone variable to get rid of warnings
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


#Get Data for polish procurement data from a public API
url = "https://tenders.guru/api/pl/tenders"
try:
    r = requests.get(url)
except requests.ConnectionError as ce:
    logging.error(f"There was an error with the request, {ce}")
    sys.exit(1)

#Make a session of Spark to load data into S3
findspark.init()

spark = SparkSession.builder \
    .appName("PySpark_to_S3") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "##################") \ #S3 Access key
    .config("spark.hadoop.fs.s3a.secret.key", "################################") \ #S3 secret key
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

#Parallelize used to create a Resilient Distributed Dataset (RDD) from a local Python collection (like a list or array)
# Used to distribute a local data collection across the nodes of a Spark cluster
rdd = spark.sparkContext.parallelize([r.text])

#Save raw file as a string to convert it to json and go through it later
rdd.saveAsTextFile("s3a://polish-api-to-s3/polish-data.txt")

spark.stop()
