# Telecom Customer Value and Digital Billing Analytics

An exam-ready, near-real-time data platform for telecom billing and customer-value analytics.

## What the system demonstrates

`Customer seed data/script/Postman -> Django REST API -> Kafka -> Kafka Connect -> HDFS + MySQL (essentials) -> PySpark/MLlib -> Django`

The supplied workbook is used only as customer master data. The generator creates realistic, timestamped billing, payment, usage, support, and plan-change events. This makes the data replayable and suitable for streaming demonstrations.

## Repository layout

```text
src/generator/       Rapid generator that calls Django REST
src/consumer/        Manual-commit consumer, operations summary, and alerts
spark/               HDFS insights, MLlib training, and scoring jobs
connect/             Kafka Connect sink configurations
dashboard/           Django operational and analytics dashboard
docs/                Report, presentation, and demo checklists
```

## Quick start

This project does not use Docker. It requires local Kafka, Kafka Connect, MySQL, Spark, and pseudo-distributed HDFS. On Windows, use WSL2 for the Hadoop/Kafka/Spark services.

1. Copy `.env.example` to `.env` and configure local credentials.
2. Copy the supplied workbook to `data/Dataset1_Telecommunications.xlsx`.
3. Follow the complete [native setup guide](docs/local-setup.md) to start HDFS, Kafka, Connect, and MySQL.
4. Start Django: `python dashboard/manage.py runserver`.
5. Start the generator: `python src/generator/telecom_event_generator.py --seed data/Dataset1_Telecommunications.xlsx --rate 20`.
6. Start two consumers in separate terminals: `python src/consumer/operational_consumer.py --name consumer-a` and the same command with `--name consumer-b`.
7. Submit `spark/batch_insights.py` and `spark/train_segmenter.py`. Score only post-cutoff activity with `spark/score_new_activity.py --cutoff 2026-09-15T10:00:00+00:00`.

See `docs/demo-checklist.md` for the live-defense sequence and `docs/report-outline.md` for the written report.

## Kafka design decisions

- Topic: `telecom.events`, three partitions.
- Partition key: `customer_id`. Events for one customer remain ordered while the workload distributes across partitions.
- Custom consumer group: `telecom-operations-v1`; auto commit is disabled. A record is committed only after its MySQL operational-summary/alert transaction succeeds, providing at-least-once processing. Processed event IDs are stored transactionally, so a Kafka replay cannot double-count operational summaries.
- Kafka Connect sinks the full JSON event history to HDFS and raw events to MySQL database `essentials` automatically. The custom consumer exists to demonstrate consumer groups, offsets, lag, rebalance behaviour.

## ML choice

Spark MLlib K-Means segments customers using tenure, recurring charge, support load, and generated payment/usage behaviour. This is appropriate because the supplied file contains no churn examples and `TotalCharges` is deterministically derived from tenure and monthly charge. Spark publishes the scored segments to MySQL and the dashboard presents them beside consumer-generated billing alerts.

## Important environment variables

Copy `.env.example` to `.env`. `KAFKA_BOOTSTRAP_SERVERS` defaults to `localhost:9092`; `MYSQL_DATABASE` defaults to `essentials`. Never commit `.env` or a real database password.
