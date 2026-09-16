"""Batch-deploy the saved MLlib model to activity that arrived after a chosen cutoff."""
import argparse
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pyspark.sql.functions import avg, col, count, current_timestamp, lit, sum as total, to_timestamp, when

load_dotenv()
parser = argparse.ArgumentParser()
parser.add_argument("--cutoff", required=True, help="UTC ISO timestamp, for example 2026-09-15T10:00:00+00:00")
args = parser.parse_args()

spark = SparkSession.builder.appName("telecom-segmentation-scoring").getOrCreate()
hdfs_url = os.getenv("HDFS_URL", "hdfs://localhost:9000")
new_events = spark.read.json(f"{hdfs_url}/telecom/events/telecom.events/*").filter(
    to_timestamp(col("event_time")) > to_timestamp(lit(args.cutoff))
)
features = new_events.groupBy("customer_id").agg(
    avg("customer.tenure_months").alias("tenure_months"),
    avg(when(col("event_type") == "invoice", col("payload.amount"))).alias("monthly_charge"),
    count(when(col("event_type") == "support_call", True)).alias("support_calls"),
    total(when(col("event_type") == "usage", col("payload.data_gb")).otherwise(0)).alias("usage_gb"),
    count(when((col("event_type") == "payment") & (col("payload.status") == "failed"), True)).alias("failed_payments"),
).fillna(0)
model = PipelineModel.load(f"{hdfs_url}/telecom/models/customer_segmenter")
scores = model.transform(features).select("customer_id", col("prediction").alias("segment"), current_timestamp().alias("scored_at"))
scores.write.mode("overwrite").parquet(f"{hdfs_url}/telecom/analytics/latest_segment_scores")
scores.write.mode("overwrite").jdbc(os.getenv("MYSQL_JDBC_URL", "jdbc:mysql://localhost:3306/essentials"), "customer_segment_scores", properties={"user": os.getenv("MYSQL_USER", "telecom_app"), "password": os.getenv("MYSQL_PASSWORD"), "driver": "com.mysql.cj.jdbc.Driver"})
spark.stop()
