# 5-8 page report outline

1. Problem statement: detect billing risk and segment telecom customers for retention action.
2. Actual architecture diagram: generator, Kafka, Connect, HDFS, MySQL `essentials`, Spark, MLlib, Django.
3. Data schema: customer seed fields and generated event JSON contract.
4. Streaming and storage design: customer-ID partitioning, consumer group, manual offsets, HDFS date partitions, and MySQL operational role.
5. PySpark insights: event volume, payment-failure rate/value, and usage by service.
6. MLlib: K-Means features, chosen k, silhouette evaluation, segment meaning, and batch scoring.
7. Dashboard and enhancement: live operations plus failed-payment alert workflow.
8. Limitations and future work: synthetic events, single-node classroom cluster, and richer labelled churn history.
