"""
Continuously publish synthetic transaction events to Kafka.

Usage:
    python -m producer.produce_transactions
    python -m producer.produce_transactions --rate 5 --duplicates --invalids
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from confluent_kafka import Producer

from config.settings import settings

# Graceful shutdown
_running = True


def _handle_signal(signum: int, frame: Any) -> None:
    global _running
    _running = False
    print("\nStopping producer...")


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Sent key={msg.key().decode() if msg.key() else None} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def make_event(
    transaction_id: int,
    *,
    invalid: bool = False,
    late_seconds: int = 0,
) -> dict[str, Any]:
    """Build one transaction event. Invalid events violate schema on purpose."""
    now = datetime.now(timezone.utc) - timedelta(seconds=late_seconds)
    if invalid:
        # Missing required fields / bad types for consumer & Spark validation.
        kind = random.choice(["missing_amount", "bad_customer", "null_id"])
        if kind == "missing_amount":
            return {
                "transaction_id": transaction_id,
                "customer_id": random.randint(1, 50),
                "timestamp": now.isoformat(),
            }
        if kind == "bad_customer":
            return {
                "transaction_id": transaction_id,
                "customer_id": "not-an-int",
                "amount": round(random.uniform(1, 500), 2),
                "timestamp": now.isoformat(),
            }
        return {
            "transaction_id": None,
            "customer_id": random.randint(1, 50),
            "amount": round(random.uniform(1, 500), 2),
            "timestamp": now.isoformat(),
        }

    return {
        "transaction_id": transaction_id,
        "customer_id": random.randint(1, 50),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "currency": "USD",
        "merchant": random.choice(
            ["Amazon", "Walmart", "Starbucks", "Uber", "Apple", "Target"]
        ),
        "timestamp": now.isoformat(),
        "event_id": str(uuid.uuid4()),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka transaction event producer")
    parser.add_argument("--bootstrap", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_topic)
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second")
    parser.add_argument("--start-id", type=int, default=1001)
    parser.add_argument(
        "--duplicates",
        action="store_true",
        help="Occasionally re-send the previous event (duplicate)",
    )
    parser.add_argument(
        "--invalids",
        action="store_true",
        help="Occasionally send schema-invalid events",
    )
    parser.add_argument(
        "--late",
        action="store_true",
        help="Occasionally send late-arriving events (old timestamps)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after N events (0 = run forever)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
        }
    )

    print(f"Producing to {args.topic} @ {args.bootstrap} (~{args.rate} evt/s)")
    print("Press Ctrl+C to stop.\n")

    txn_id = args.start_id
    sent = 0
    last_payload: bytes | None = None
    last_key: str | None = None
    delay = 1.0 / args.rate if args.rate > 0 else 0.5

    while _running:
        if args.max_events and sent >= args.max_events:
            break

        # ~8% duplicates when enabled
        if args.duplicates and last_payload and random.random() < 0.08:
            producer.produce(
                args.topic,
                key=last_key,
                value=last_payload,
                callback=delivery_report,
            )
            print("  (duplicate resent)")
        else:
            invalid = args.invalids and random.random() < 0.1
            late_seconds = random.randint(120, 600) if args.late and random.random() < 0.1 else 0
            event = make_event(txn_id, invalid=invalid, late_seconds=late_seconds)
            key = str(event.get("customer_id") or "unknown")
            payload = json.dumps(event).encode("utf-8")
            producer.produce(
                args.topic,
                key=key,
                value=payload,
                callback=delivery_report,
            )
            last_payload = payload
            last_key = key
            if not invalid:
                txn_id += 1
            if late_seconds:
                print(f"  (late by {late_seconds}s)")
            if invalid:
                print("  (invalid event)")

        producer.poll(0)
        sent += 1
        time.sleep(delay)

    producer.flush(10)
    print(f"Producer stopped after {sent} publish attempts.")


if __name__ == "__main__":
    main()
