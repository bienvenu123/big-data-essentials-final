"""Read historical JSON events from HDFS and write three analytical outputs."""
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pyspark.sql.functions import avg, col, count, lit, sum as total, when

load_dotenv()
spark = SparkSession.builder.appName("telecom-batch-insights").getOrCreate()
hdfs_url = os.getenv("HDFS_URL", "hdfs://localhost:9000")
events = spark.read.json(f"{hdfs_url}/telecom/events/telecom.events/*")

# Insight 1: event volume by type. Insight 2: payment-failure rate. Insight 3: usage by service.
def publish(frame, table):
    frame.write.mode("overwrite").jdbc(os.getenv("MYSQL_JDBC_URL", "jdbc:mysql://localhost:3306/essentials"), table, properties={"user": os.getenv("MYSQL_USER", "telecom_app"), "password": os.getenv("MYSQL_PASSWORD"), "driver": "com.mysql.cj.jdbc.Driver"})

event_volume = events.groupBy("event_type").agg(count("event_id").alias("events"))
event_volume.write.mode("overwrite").parquet(f"{hdfs_url}/telecom/analytics/event_volume")
publish(event_volume, "analytics_event_volume")
payments = events.filter(col("event_type") == "payment")
payment_failure_rate = payments.agg(
    count("event_id").alias("payments"),
    total(when(col("payload.status") == "failed", lit(1)).otherwise(lit(0))).alias("failed_payments"),
).withColumn("failure_rate", when(col("payments") > 0, col("failed_payments") / col("payments")).otherwise(lit(0.0)))
payment_failure_rate.write.mode("overwrite").parquet(f"{hdfs_url}/telecom/analytics/payment_failure_rate")
publish(payment_failure_rate, "analytics_payment_failure_rate")
usage = events.filter(col("event_type") == "usage").groupBy("customer.internet_service").agg(avg("payload.data_gb").alias("average_data_gb"), total("payload.minutes").alias("minutes"))
usage.write.mode("overwrite").parquet(f"{hdfs_url}/telecom/analytics/usage_by_service")
publish(usage, "analytics_usage_by_service")
spark.stop()
