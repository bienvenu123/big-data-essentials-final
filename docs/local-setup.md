# Local setup without Docker

This project keeps HDFS because it is mandatory for the assignment. On Windows, run Hadoop, Kafka, Kafka Connect, and Spark inside WSL2 (Ubuntu); run Django and the generator either in WSL2 or Windows, as long as their environment variables point to the same services.

## 1. Project configuration

From the repository root, copy `.env.example` to `.env`, then set a real local MySQL password and Django secret. Do not commit `.env`.

Create the database and least-privilege application account in MySQL:

```sql
CREATE DATABASE essentials;
CREATE USER 'telecom_app'@'localhost' IDENTIFIED BY 'your-local-password';
GRANT ALL PRIVILEGES ON essentials.* TO 'telecom_app'@'localhost';
FLUSH PRIVILEGES;
```

Install Python dependencies with `python -m pip install -r requirements.txt`.

## 2. Start HDFS in WSL2

Install a compatible Java LTS release and Apache Hadoop. Configure Hadoop in pseudo-distributed mode: `fs.defaultFS` must be `hdfs://localhost:9000`; use a local NameNode/DataNode directory; and set an HDFS replication factor of `1` for this one-machine demonstration. Format the NameNode once, then start DFS:

```bash
hdfs namenode -format
start-dfs.sh
hdfs dfs -mkdir -p /telecom/events /telecom/analytics /telecom/models
hdfs dfs -ls /
```

Keep the Hadoop configuration directory in `HADOOP_CONF_DIR` when submitting Spark jobs. For a classroom cluster, increase replication and use the institution's HDFS endpoint instead.

## 3. Start Kafka and Kafka Connect in WSL2

Install Apache Kafka and start one broker in KRaft mode using Kafka's supplied local configuration. Create the required topic:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic telecom.events --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic telecom.events
```

Install the Confluent JDBC Sink Connector and HDFS Sink Connector into Kafka Connect's plugin path. Configure the Connect worker with the `EnvVarConfigProvider` and make `MYSQL_USER` and `MYSQL_PASSWORD` available to that worker; this resolves the `${env:...}` values in `connect/mysql-sink.json`. Start the worker, then register both connectors:

```bash
curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' --data-binary @connect/mysql-sink.json
curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' --data-binary @connect/hdfs-sink.json
curl http://localhost:8083/connectors?expand=status
```

The HDFS connector must be able to reach `hdfs://localhost:9000`. If Connect runs inside WSL2, use WSL's `localhost`; if it runs elsewhere, set a reachable HDFS host in `connect/hdfs-sink.json`.

## 4. Run the application

In separate terminals from the repository root:

```powershell
python dashboard/manage.py runserver
python src/generator/telecom_event_generator.py --seed data/Dataset1_Telecommunications.xlsx --rate 20
python src/consumer/operational_consumer.py --name consumer-a
python src/consumer/operational_consumer.py --name consumer-b
```

Inspect consumer assignment and lag during the demonstration:

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group telecom-operations-v1
```

Stop one consumer, observe reassignment, then restart it. The consumer writes each `event_id` to `processed_events` in the same MySQL transaction as the operational update, preventing duplicate counter updates after a replay.

## 5. Run Spark jobs

Install a Spark release compatible with the Java and Hadoop versions. Submit jobs with the MySQL JDBC driver available to Spark and point Spark at the HDFS configuration:

```bash
spark-submit --packages com.mysql:mysql-connector-j:8.4.0 spark/batch_insights.py
spark-submit --packages com.mysql:mysql-connector-j:8.4.0 spark/train_segmenter.py
spark-submit --packages com.mysql:mysql-connector-j:8.4.0 spark/score_new_activity.py --cutoff 2026-09-15T10:00:00+00:00
```

Verify that event history and analytical outputs exist in HDFS:

```bash
hdfs dfs -ls -R /telecom/events
hdfs dfs -ls -R /telecom/analytics
```
