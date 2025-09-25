from pyspark.sql import SparkSession
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import findspark
import json
import os
from pyspark.sql.functions import col,from_json, explode

#Set timezone variable to get rid of warnings
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

#Make a session of Spark to load data into S3
findspark.init()

spark = SparkSession.builder \
    .appName("Clean_Tender_JSON") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAVUIFVRGGIGPRHUZW") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.jars", "/opt/spark/jars/redshift-jdbc4-1.2.45.1069.jar") \
    .getOrCreate()


from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    ArrayType, IntegerType, MapType, DoubleType
)

# Create a schema for the txt file queried from s3
# Supplier schema (nested in 'awarded')
supplier_schema = StructType([
    StructField("id", IntegerType()),
    StructField("slug", StringType()),
    StructField("name", StringType())
])

# Offers count data - subschema of data
offers_count_data_schema = MapType(
    StringType(),
    StructType([
        StructField("count", StringType()),
        StructField("value", StringType()),
        StructField("value_eur", DoubleType())
    ])
)

# Awarded schema - subschema of data
awarded_schema = StructType([
    StructField("date", StringType()),
    StructField("suppliers_id", StringType()),
    StructField("count", StringType()),
    StructField("value", StringType()),
    StructField("value_min", StringType()),
    StructField("value_max", StringType()),
    StructField("value_estimated", StringType()),
    StructField("suppliers_name", StringType()),
    StructField("suppliers", ArrayType(supplier_schema)),
    StructField("value_for_one", IntegerType()),
    StructField("value_for_two", IntegerType()),
    StructField("value_for_three", IntegerType()),
    StructField("offers_count_data", offers_count_data_schema),
    StructField("offers_count", ArrayType(IntegerType())),
    StructField("value_eur", DoubleType()),
    StructField("value_for_one_eur", DoubleType()),
    StructField("value_for_two_eur", DoubleType()),
    StructField("value_for_three_eur", DoubleType())
])

# Purchaser schema - subschema of data
purchaser_schema = StructType([
    StructField("id", StringType()),
    StructField("sid", StringType()),
    StructField("name", StringType())
])

# Type schema - subschema of data
type_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("slug", StringType())
])

# Data element (one tender)
data_item_schema = StructType([
    StructField("id", IntegerType()),
    StructField("date", StringType()),
    StructField("title", StringType()),
    StructField("category", StringType()),
    StructField("description", StringType()),
    StructField("sid", StringType()),
    StructField("awarded_value", StringType()),
    StructField("awarded_currency", StringType()),
    StructField("awarded_value_eur", DoubleType()),
    StructField("purchaser", purchaser_schema),
    StructField("type", type_schema),
    StructField("awarded", ArrayType(awarded_schema))
])

# Full top-level schema
full_schema = StructType([
    StructField("page_count", IntegerType()),
    StructField("page_number", IntegerType()),
    StructField("page_size", IntegerType()),
    StructField("total", IntegerType()),
    StructField("data", ArrayType(data_item_schema))
])


# Read text file from S3
txt_df = spark.read.text("s3a://polish-api-to-s3/polish-data.txt")

# Parse each line as JSON using schema
json_df = txt_df.select(from_json(col("value"), full_schema).alias("json"))

# Flatten 'data' array
data_df = json_df.select(explode("json.data").alias("tender"))

# Clean the data and pick out the columns needed
clean_df = data_df.select(
    col("tender.id").alias("id"),
    col("tender.title").alias("title"),
    col("tender.category").alias("category"),
    col("tender.description").alias("description"),
    col("tender.awarded_value_eur").alias("awarded_value_eur")
)

#Connecting to Redshift to load data into a table
#Syntax: jdbc:redshift://workgroupName.AWS_AccountID.us-east-2.redshift-serverless.amazonaws.com:5439/database
redshift_url = "jdbc:redshift://polish-data-processing.############.us-east-2.redshift-serverless.amazonaws.com:5439/dev"
connection_properties = {
    "user": "############", #Redshift database username
    "password": "###################", #Redshift database user password
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

clean_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "data") \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .option("driver", connection_properties["driver"]) \
    .mode("append") \
    .save()

spark.stop()
