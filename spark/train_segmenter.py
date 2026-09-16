"""Train and evaluate a Spark MLlib customer-segmentation model from HDFS event history."""
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pyspark.sql.functions import avg, col, count, current_timestamp, sum as total, when

load_dotenv()
spark = SparkSession.builder.appName("telecom-segmentation-training").getOrCreate()
hdfs_url = os.getenv("HDFS_URL", "hdfs://localhost:9000")
events = spark.read.json(f"{hdfs_url}/telecom/events/telecom.events/*")
features = events.groupBy("customer_id").agg(
    avg("customer.tenure_months").alias("tenure_months"),
    avg(when(col("event_type") == "invoice", col("payload.amount"))).alias("monthly_charge"),
    count(when(col("event_type") == "support_call", True)).alias("support_calls"),
    total(when(col("event_type") == "usage", col("payload.data_gb")).otherwise(0)).alias("usage_gb"),
    count(when((col("event_type") == "payment") & (col("payload.status") == "failed"), True)).alias("failed_payments"),
).fillna(0)
assembler = VectorAssembler(inputCols=["tenure_months", "monthly_charge", "support_calls", "usage_gb", "failed_payments"], outputCol="features")
model = Pipeline(stages=[assembler, KMeans(k=4, seed=42)]).fit(features)
predictions = model.transform(features)
score = ClusteringEvaluator().evaluate(predictions)
print(f"Silhouette score: {score:.4f}")
model.write().overwrite().save(f"{hdfs_url}/telecom/models/customer_segmenter")
segment_scores = predictions.select("customer_id", col("prediction").alias("segment"), current_timestamp().alias("scored_at"))
segment_scores.write.mode("overwrite").parquet(f"{hdfs_url}/telecom/analytics/customer_segments")
segment_scores.write.mode("overwrite").jdbc(os.getenv("MYSQL_JDBC_URL", "jdbc:mysql://localhost:3306/essentials"), "customer_segment_scores", properties={"user": os.getenv("MYSQL_USER", "telecom_app"), "password": os.getenv("MYSQL_PASSWORD"), "driver": "com.mysql.cj.jdbc.Driver"})
spark.stop()
