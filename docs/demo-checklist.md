# Live demo checklist

1. Start Compose and show Kafka, HDFS NameNode, MySQL database `essentials`, and Connect are running.
2. Open Django at port 8000, show its REST endpoint, then start the generator at 20+ events/second. The generator calls Django REST; Django publishes to Kafka.
3. Start `consumer-a` and `consumer-b` in group `telecom-operations-v1`; show partition assignment using `kafka-consumer-groups --describe`.
4. Stop one consumer. Show reassignment and lag, restart it, then explain manual commits and at-least-once delivery.
5. Show both Kafka Connect connector statuses and automatic HDFS/MySQL records.
6. Run Spark insights from HDFS and show three outputs.
7. Train K-Means, state its silhouette score, and show newly scored segments.
8. Open the Django dashboard. Explain that the custom consumer creates the operational summary and failed-payment notifications, while Spark loads ML segment scores into MySQL `essentials` for the analytics view.
