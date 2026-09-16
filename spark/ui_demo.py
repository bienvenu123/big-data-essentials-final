"""Keep a completed Spark job visible in the Spark UI for a short demo window."""
import time

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("telecom-spark-ui-demo").getOrCreate()
spark.range(1_000_000).groupBy().count().show()
print("Spark UI demo is available for five minutes.", flush=True)
time.sleep(300)
spark.stop()
