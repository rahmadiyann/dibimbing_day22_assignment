import os
from pathlib import Path
import pyspark
from pyspark.sql.functions import (
    col, count, sum, avg, lag, dense_rank,
    datediff, when
)
from pyspark.sql.window import Window
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
    .setAppName('OlistAnalysis')
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

# Read from PostgreSQL
customer_orders = spark.read.jdbc(
    jdbc_url,
    'public.olist_customer_orders',
    properties=jdbc_properties
)

# 1. Customer Segmentation Analysis
customer_segments = customer_orders.groupBy(
    "customer_unique_id"
).agg(
    sum("price").alias("total_spent"),
    count("order_id").alias("order_count"),
    avg("price").alias("avg_order_value")
)

# Create RFM segments
windowSpec = Window.orderBy("total_spent")
customer_segments = customer_segments.withColumn(
    "spending_rank",
    dense_rank().over(windowSpec)
)

customer_segments = customer_segments.withColumn(
    "customer_segment",
    when(col("spending_rank") <= 100, "Premium")
    .when(col("spending_rank") <= 1000, "High Value")
    .when(col("spending_rank") <= 5000, "Medium Value")
    .otherwise("Low Value")
)

# 2. Customer Retention Analysis
orders_by_customer = customer_orders.select(
    "customer_unique_id",
    "order_purchase_timestamp"
).distinct()

window_spec = Window.partitionBy("customer_unique_id").orderBy("order_purchase_timestamp")
customer_retention = orders_by_customer.withColumn(
    "previous_order",
    lag("order_purchase_timestamp").over(window_spec)
)

customer_retention = customer_retention.withColumn(
    "days_between_orders",
    datediff(col("order_purchase_timestamp"), col("previous_order"))
)

retention_metrics = customer_retention.filter(
    col("days_between_orders").isNotNull()
).agg(
    avg("days_between_orders").alias("avg_days_between_orders"),
    count("*").alias("repeat_purchases")
)

# 3. Write results to PostgreSQL
(
    customer_segments
    .write
    .mode("overwrite")
    .jdbc(
        jdbc_url,
        'public.olist_customer_segments',
        properties=jdbc_properties
    )
)

# Save retention metrics
(
    retention_metrics
    .write
    .mode("overwrite")
    .jdbc(
        jdbc_url,
        'public.olist_retention_metrics',
        properties=jdbc_properties
    )
)

# Print analysis results
print("\n=== Customer Segmentation Summary ===")
customer_segments.groupBy("customer_segment").agg(
    count("*").alias("customer_count"),
    avg("total_spent").alias("avg_total_spent")
).show()

print("\n=== Retention Metrics ===")
retention_metrics.show() 