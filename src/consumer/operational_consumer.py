"""Manual-commit consumer used in the live consumer-group/rebalance demonstration."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime

from confluent_kafka import Consumer, KafkaException
import pymysql
from dotenv import load_dotenv

load_dotenv()


def apply_business_logic(connection, record: dict) -> None:
    """Apply each event once, even if Kafka redelivers after an offset-commit failure."""
    # A consumer receives a Kafka record and turns it into operational MySQL data.
    # Both consumer-a and consumer-b execute this same function.
    payload = record["payload"]
    is_failed_payment = record["event_type"] == "payment" and payload.get("status") == "failed"
    event_time = datetime.fromisoformat(record["event_time"].replace("Z", "+00:00")).replace(tzinfo=None)
    with connection.cursor() as cursor:
        # Kafka uses at-least-once delivery: an event can be delivered again if
        # processing succeeds but the offset commit does not. Store event_id first
        # so a replay cannot increment the same customer's counters twice.
        cursor.execute("INSERT IGNORE INTO processed_events (event_id, processed_at) VALUES (%s, NOW(6))", (record["event_id"],))
        if cursor.rowcount == 0:
            connection.commit()
            return
        # There is one summary row per customer. A new event inserts the row or
        # updates it, increasing events_processed and failed-payment counts.
        cursor.execute("""
            INSERT INTO customer_operations (customer_id, last_event_time, last_event_type, payment_status, payment_amount, failed_payments, alert_level, events_processed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE
              last_event_time = VALUES(last_event_time),
              last_event_type = VALUES(last_event_type),
              payment_status = COALESCE(VALUES(payment_status), payment_status),
              payment_amount = COALESCE(VALUES(payment_amount), payment_amount),
              failed_payments = failed_payments + VALUES(failed_payments),
              alert_level = CASE WHEN failed_payments + VALUES(failed_payments) >= 2 THEN 'high' ELSE VALUES(alert_level) END,
              events_processed = events_processed + 1
        """, (record["customer_id"], event_time, record["event_type"], payload.get("status"), payload.get("amount"), int(is_failed_payment), "high" if is_failed_payment else "normal"))
        if is_failed_payment:
            # Failed payments also create an alert for the dashboard.
            cursor.execute("""
                INSERT INTO billing_alerts (event_id, customer_id, alert_type, message, created_at)
                VALUES (%s, %s, 'failed_payment', %s, %s) ON DUPLICATE KEY UPDATE event_id = VALUES(event_id)
            """, (record["event_id"], record["customer_id"], f"Payment failed: {payload.get('amount', 0)}", event_time))
    connection.commit()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    args = parser.parse_args()
    with pymysql.connect(host=os.getenv("MYSQL_HOST", "localhost"), port=int(os.getenv("MYSQL_PORT", "3306")), user=os.getenv("MYSQL_USER", "telecom_app"), password=os.getenv("MYSQL_PASSWORD"), database=os.getenv("MYSQL_DATABASE", "essentials"), autocommit=False) as database:
        with database.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customer_operations (
                  customer_id BIGINT PRIMARY KEY, last_event_time DATETIME(6), last_event_type VARCHAR(40),
                  payment_status TEXT, payment_amount NUMERIC, failed_payments INTEGER NOT NULL DEFAULT 0,
                  alert_level VARCHAR(20) NOT NULL DEFAULT 'normal', events_processed BIGINT NOT NULL DEFAULT 0
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS billing_alerts (
                  event_id CHAR(36) PRIMARY KEY, customer_id BIGINT NOT NULL, alert_type VARCHAR(40) NOT NULL,
                  message TEXT NOT NULL, created_at DATETIME(6) NOT NULL, resolved BOOLEAN NOT NULL DEFAULT FALSE
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                  event_id CHAR(36) PRIMARY KEY, processed_at DATETIME(6) NOT NULL
                )
            """)
        database.commit()
        # Keep one MySQL connection open while this consumer instance runs.
        consume(args, database)


def consume(args, database) -> None:
    # consumer-a and consumer-b have different client IDs but share one group ID.
    # Kafka assigns each topic partition to only one member of that group.
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": "telecom-operations-v1",
        "client.id": args.name,
        # Offsets are committed manually only after MySQL processing succeeds.
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    # Subscribe to the topic. Kafka handles partition assignment and rebalances.
    consumer.subscribe(["telecom.events"])
    print(f"{args.name} started. Stop this process to demonstrate a rebalance.")
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            # Convert Kafka's JSON bytes back into the Python event dictionary.
            record = json.loads(message.value())
            apply_business_logic(database, record)
            print(f"{args.name}: partition={message.partition()} offset={message.offset()} event={record['event_type']}")
            # Commit only after the MySQL summary/alert transaction succeeds.
            # This gives at-least-once delivery; processed_events handles replays.
            consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
