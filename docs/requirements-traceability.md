# Assignment requirement traceability

| Requirement | Implementation evidence |
|---|---|
| Rapid configurable data | `telecom_event_generator.py --rate` calls Django REST |
| Django REST producer, topics, partitions, key | `/api/events/`; `telecom.events`; 3 partitions; `customer_id` key |
| Consumer group, offsets, rebalance | `operational_consumer.py`; `telecom-operations-v1`; manual commit |
| Kafka Connect automatic sinks | `connect/mysql-sink.json`, `connect/hdfs-sink.json` |
| HDFS historical storage | date-partitioned Connect HDFS sink |
| Operational relational storage | MySQL `essentials` JDBC raw-event sink; custom consumer maintains summary/alerts |
| Spark insights | `spark/batch_insights.py` |
| Spark MLlib train/evaluate/deploy/score | `train_segmenter.py`, `score_new_activity.py` |
| Django dashboard and enhancement | operational summaries, consumer billing alerts, and ML segment scores |
| Documentation | `README.md`, report outline, architecture, demo checklist |
