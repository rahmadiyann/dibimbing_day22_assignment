import os
from pathlib import Path
import pyspark
from pyspark.sql.functions import (
    to_timestamp, date_format, count, sum, avg
)
from dotenv import load_dotenv

# Load environment variables
dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

spark_host = "spark://dibimbing-dataeng-spark-master:7077"

# Initialize Spark
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
    pyspark
    .SparkConf()
    .setAppName('OlistETL')
    .setMaster(spark_host)
    .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")
))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# Database connection properties
jdbc_url = f'jdbc:postgresql://{postgres_host}:5432/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Read and process orders data
orders_df = spark.read.csv(
    "/data/olist/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

# Read and process customers data
customers_df = spark.read.csv(
    "/data/olist/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)

# Read and process order items data
order_items_df = spark.read.csv(
    "/data/olist/olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)

# Process and transform data
orders_processed = orders_df.withColumn(
    "order_purchase_timestamp",
    to_timestamp("order_purchase_timestamp")
).withColumn(
    "order_year_month",
    date_format("order_purchase_timestamp", "yyyy-MM")
)

# Join datasets
customer_orders = orders_processed.join(
    customers_df,
    "customer_id"
).join(
    order_items_df,
    "order_id"
)

# Create customer purchase summary
customer_summary = customer_orders.groupBy(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state"
).agg(
    count("order_id").alias("total_orders"),
    sum("price").alias("total_spent"),
    avg("price").alias("avg_order_value")
)

# Write to PostgreSQL
(
    customer_summary
    .write
    .mode("overwrite")
    .jdbc(
        jdbc_url,
        'public.olist_customer_summary',
        properties=jdbc_properties
    )
)

# Write orders data
(
    customer_orders
    .write
    .mode("overwrite")
    .jdbc(
        jdbc_url,
        'public.olist_customer_orders',
        properties=jdbc_properties
    )
)

print("ETL process completed successfully!") 