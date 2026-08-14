"""
Kafka consumer: parse, validate, and handle invalid messages.

Valid events are printed to stdout. Invalid events are:
  1. Logged with a reason
  2. Forwarded to the dead-letter topic (transactions.dlq)

Usage:
    python -m consumer.consume_transactions
"""

from __future__ import annotations

import argparse
import json
import signal
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from config.settings import settings

_running = True


def _handle_signal(signum: int, frame: Any) -> None:
    global _running
    _running = False
    print("\nStopping consumer...")


REQUIRED_FIELDS = ("transaction_id", "customer_id", "amount", "timestamp")


def validate_event(payload: dict[str, Any]) -> tuple[bool, str | None]:
    """Return (ok, error_reason)."""
    for field in REQUIRED_FIELDS:
        if field not in payload or payload[field] is None:
            return False, f"missing_or_null_field:{field}"

    try:
        int(payload["transaction_id"])
        int(payload["customer_id"])
        amount = float(payload["amount"])
    except (TypeError, ValueError) as exc:
        return False, f"type_error:{exc}"

    if amount <= 0:
        return False, "amount_must_be_positive"

    try:
        # Accept both "Z" and offset ISO formats.
        ts = str(payload["timestamp"]).replace("Z", "+00:00")
        datetime.fromisoformat(ts)
    except ValueError:
        return False, "invalid_timestamp"

    return True, None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka transaction consumer")
    parser.add_argument("--bootstrap", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_topic)
    parser.add_argument("--dlq", default=settings.kafka_dlq_topic)
    parser.add_argument("--group", default=settings.kafka_consumer_group)
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Start from earliest offset (new consumer group)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": True,
        }
    )
    dlq_producer = Producer({"bootstrap.servers": args.bootstrap, "acks": "all"})
    consumer.subscribe([args.topic])

    print(f"Consuming {args.topic} as group={args.group}")
    print(f"Invalid messages -> {args.dlq}")
    print("Press Ctrl+C to stop.\n")

    ok_count = 0
    bad_count = 0

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            raw = msg.value()
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                bad_count += 1
                print(f"[INVALID] offset={msg.offset()} reason=json_parse:{exc}")
                dlq_producer.produce(
                    args.dlq,
                    key=msg.key(),
                    value=json.dumps(
                        {
                            "error": f"json_parse:{exc}",
                            "raw": raw.decode("utf-8", errors="replace"),
                            "source_partition": msg.partition(),
                            "source_offset": msg.offset(),
                        }
                    ).encode("utf-8"),
                )
                dlq_producer.poll(0)
                continue

            valid, reason = validate_event(payload)
            if not valid:
                bad_count += 1
                print(
                    f"[INVALID] offset={msg.offset()} reason={reason} payload={payload}"
                )
                dlq_producer.produce(
                    args.dlq,
                    key=msg.key(),
                    value=json.dumps(
                        {
                            "error": reason,
                            "payload": payload,
                            "source_partition": msg.partition(),
                            "source_offset": msg.offset(),
                        }
                    ).encode("utf-8"),
                )
                dlq_producer.poll(0)
                continue

            ok_count += 1
            print(
                f"[OK] txn={payload['transaction_id']} "
                f"customer={payload['customer_id']} "
                f"amount={payload['amount']} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )
    finally:
        dlq_producer.flush(5)
        consumer.close()
        print(f"\nDone. valid={ok_count} invalid={bad_count}")


if __name__ == "__main__":
    main()
