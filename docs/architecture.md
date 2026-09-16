# Actual architecture

```mermaid
flowchart LR
    Seed[Telecommunications workbook] --> Gen[Python script or Postman]
    Gen --> REST[Django REST producer]
    REST -->|customer_id key| Kafka[Kafka: telecom.events, 3 partitions]
    Kafka --> Custom[Custom consumer: summaries and alerts]
    Kafka --> Connect[Kafka Connect]
    Connect --> HDFS[HDFS: replayable date partitions]
    Connect --> DB[MySQL database: essentials]
    HDFS --> Spark[PySpark insights and MLlib]
    Custom --> Django
    Spark --> Scores[Customer segment scores]
    Scores --> DB
    DB --> Django[Django live dashboard]
```

HDFS is the historical system of record for the full event stream. MySQL database `essentials` is deliberately limited to quick operational queries and dashboard-ready analytics. Spark reads HDFS directly rather than the operational database.
