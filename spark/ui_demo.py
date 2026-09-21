"""Keep a completed Spark job visible in the Spark UI until it is stopped."""
import time

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("telecom-spark-ui-demo").getOrCreate()
spark.range(1_000_000).groupBy().count().show()
print("Spark UI demo is available until you stop this process.", flush=True)
try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    spark.stop()
