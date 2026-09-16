"""Shared event contract used by the generator, producer, consumers, and Spark."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4


def event(customer: dict, event_type: str, payload: dict) -> dict:
    # This is the shared event contract. Keeping event construction in one place
    # makes every generated message follow the same structure for Kafka consumers.
    return {
        # event_id allows consumers to detect a replay; UTC time is used so all
        # pipeline components interpret timestamps consistently.
        "event_id": str(uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "customer_id": int(customer["CustomerID"]),
        "customer": {
            "contract_type": customer["ContractType"],
            "internet_service": customer["InternetService"],
            "payment_method": customer["PaymentMethod"],
            "tenure_months": int(customer["Tenure"]),
        },
        "payload": payload,
    }
