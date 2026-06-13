#!/usr/bin/env python3
"""Seed 90 days of historical financial events into Bronze Delta tables."""
import os
import sys
import random
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app")

from src.python.utils.spark_session import get_spark_session
from src.python.ingestion.event_types import (
    PaymentEvent, RefundEvent, ChargebackEvent, SettlementEvent
)
from src.python.pipeline.bronze.ingest_transactions import ingest_batch
from src.python.pipeline.bronze.ingest_reference import seed_merchant_reference
from src.python.utils.logging_config import get_logger

DAYS = int(os.getenv("SEED_DAYS", "90"))
EVENTS_PER_DAY = int(os.getenv("SEED_EVENTS_PER_DAY", "5000"))
MERCHANT_COUNT = 500


def generate_day_events(day_offset: int) -> list[dict]:
    target_date = datetime.now(timezone.utc) - timedelta(days=day_offset)
    events = []
    payments_today = []

    count = int(EVENTS_PER_DAY * random.uniform(0.8, 1.2))

    for _ in range(count):
        roll = random.random()
        if roll < 0.80 or not payments_today:
            p = PaymentEvent.generate()
            # backdate timestamp
            ts = target_date.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )
            p.timestamp = ts.isoformat() + "Z"
            payments_today.append(p)
            events.append(asdict(p))
        elif roll < 0.90:
            ref = random.choice(payments_today)
            r = RefundEvent.generate(ref.transaction_id, ref.merchant_id, ref.amount)
            r.timestamp = target_date.isoformat() + "Z"
            events.append(asdict(r))
        elif roll < 0.92:
            ref = random.choice(payments_today)
            c = ChargebackEvent.generate(ref.transaction_id, ref.merchant_id, ref.amount)
            c.timestamp = target_date.isoformat() + "Z"
            events.append(asdict(c))
        else:
            ref = random.choice(payments_today) if payments_today else PaymentEvent.generate()
            s = SettlementEvent.generate(ref.merchant_id, ref.amount * random.uniform(5, 20))
            s.timestamp = target_date.isoformat() + "Z"
            events.append(asdict(s))

    return events


def main() -> None:
    logger = get_logger("seed_data", "seed")
    print(f"Seeding {DAYS} days × ~{EVENTS_PER_DAY} events/day = ~{DAYS * EVENTS_PER_DAY:,} total events")

    spark = get_spark_session(app_name="seed_data")

    print("Seeding merchant reference data...")
    seed_merchant_reference(spark, MERCHANT_COUNT)

    for day in range(DAYS, 0, -1):
        events = generate_day_events(day)
        cnt = ingest_batch(spark, events, run_id=f"seed_day_{day}")
        print(f"  Day -{day:3d}: {cnt:6,} records ingested")

    print("Seed complete.")
    spark.stop()


if __name__ == "__main__":
    main()
