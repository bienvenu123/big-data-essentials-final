import json
import os

from confluent_kafka import Producer
from django.db import connection
from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response

from .serializers import TelecomEventSerializer


def overview(request):
    """Operational view reads MySQL database essentials, never the HDFS data lake."""
    metrics = {"customers": 0, "events": 0, "failed_payments": 0}
    recent = []
    alerts = []
    segments = []
    event_volume = []
    payment_failure_rate = []
    usage_by_service = []
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM customer_operations")
            metrics["customers"] = cursor.fetchone()[0]
            cursor.execute("SELECT COALESCE(SUM(events_processed), 0) FROM customer_operations")
            metrics["events"] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM billing_alerts WHERE resolved = 0")
            metrics["failed_payments"] = cursor.fetchone()[0]
            cursor.execute("SELECT customer_id, last_event_type, last_event_time FROM customer_operations ORDER BY last_event_time DESC LIMIT 10")
            recent = cursor.fetchall()
            cursor.execute("SELECT customer_id, alert_type, message, created_at FROM billing_alerts WHERE resolved = 0 ORDER BY created_at DESC LIMIT 10")
            alerts = cursor.fetchall()
    except Exception as exc:
        metrics["database_message"] = f"Waiting for consumer operational tables: {exc}"
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT customer_id, segment, scored_at FROM customer_segment_scores ORDER BY scored_at DESC LIMIT 10")
            segments = cursor.fetchall()
            cursor.execute("SELECT event_type, events FROM analytics_event_volume ORDER BY events DESC")
            event_volume = cursor.fetchall()
            cursor.execute("SELECT payments, failed_payments, failure_rate FROM analytics_payment_failure_rate")
            payment_failure_rate = cursor.fetchall()
            cursor.execute("SELECT internet_service, average_data_gb, minutes FROM analytics_usage_by_service ORDER BY minutes DESC")
            usage_by_service = cursor.fetchall()
    except Exception as exc:
        metrics["analytics_message"] = f"Spark analytics are not loaded yet: {exc}"
    return render(request, "analytics/overview.html", {
        "metrics": metrics, "recent": recent, "alerts": alerts, "segments": segments,
        "event_volume": event_volume, "payment_failure_rate": payment_failure_rate,
        "usage_by_service": usage_by_service,
    })


@api_view(["POST"])
def ingest_event(request):
    """Django REST producer: validates the event envelope and publishes with customer-ID keying."""
    serializer = TelecomEventSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    record = serializer.validated_data
    # DateTimeField/UUIDField return Python objects; convert them to JSON-safe values.
    record["event_id"] = str(record["event_id"])
    record["event_time"] = record["event_time"].isoformat()
    delivery_error = []

    def delivery_report(error, _message):
        if error is not None:
            delivery_error.append(str(error))

    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), "acks": "all", "enable.idempotence": True})
    try:
        producer.produce("telecom.events", key=str(record["customer_id"]), value=json.dumps(record).encode(), on_delivery=delivery_report)
        outstanding = producer.flush(10)
    except BufferError as exc:
        return Response({"error": f"Kafka producer queue is full: {exc}"}, status=503)
    if outstanding or delivery_error:
        return Response({"error": "Kafka delivery failed", "detail": delivery_error}, status=503)
    return Response({"status": "accepted", "event_id": record["event_id"]}, status=202)
