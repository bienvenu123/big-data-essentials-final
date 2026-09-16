"""Generate configurable, realistic telecom events from the supplied customer workbook."""
from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.schema import event


def build_event(customer: dict) -> dict:
    # A customer is one row from the Excel seed file. Each generated message
    # represents one possible action in that customer's telecom journey.
    monthly_charge = float(customer["MonthlyCharges"])
    choice = random.choices(
        ["usage", "invoice", "payment", "support_call", "plan_change"],
        weights=[55, 15, 15, 10, 5], k=1
    )[0]
    if choice == "usage":
        return event(customer, choice, {"data_gb": round(random.uniform(0.05, 8), 2), "minutes": random.randint(0, 180)})
    if choice == "invoice":
        return event(customer, choice, {"invoice_id": f"INV-{random.randint(100000, 999999)}", "amount": monthly_charge, "status": "issued"})
    if choice == "payment":
        failed = random.random() < (0.12 if customer["ContractType"] == "Month" else 0.04)
        return event(customer, choice, {"amount": monthly_charge, "status": "failed" if failed else "paid", "method": customer["PaymentMethod"]})
    if choice == "support_call":
        return event(customer, choice, {"reason": random.choice(["billing", "network", "upgrade", "technical"]), "resolution_minutes": random.randint(3, 45)})
    return event(customer, choice, {"from_plan": customer["InternetService"], "to_plan": random.choice(["DSL", "Fiber"]), "reason": "customer_request"})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", required=True, help="Path to Dataset1_Telecommunications.xlsx")
    parser.add_argument("--rate", type=float, default=10, help="Events per second")
    parser.add_argument("--ingest-url", default="http://localhost:8000/api/events/", help="Django REST ingestion endpoint")
    parser.add_argument("--count", type=int, default=0, help="0 means run until interrupted")
    args = parser.parse_args()
    if args.rate <= 0:
        raise ValueError("--rate must be greater than zero")
    customers = pd.read_excel(args.seed).to_dict("records")
    # ``customers`` is a list of dictionaries: one dictionary per Excel row.
    # The generator repeatedly chooses one of these customers and creates an event.
    sent = 0
    try:
        while not args.count or sent < args.count:
            # The generator is a producer: it creates the event, then sends it
            # to Django. Django validates it and publishes it to Kafka.
            message = build_event(random.choice(customers))
            response = requests.post(args.ingest_url, json=message, timeout=5)
            response.raise_for_status()
            sent += 1
            if sent % int(max(args.rate, 1)) == 0:
                print(f"sent={sent}, configured_rate={args.rate}/sec")
            # Pause so --rate means approximately "events per second".
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print("Generator stopped.")


if __name__ == "__main__":
    main()
