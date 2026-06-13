import json
import os
import random
import time
from dataclasses import asdict
from typing import Callable

from confluent_kafka import Producer

from .event_types import PaymentEvent, RefundEvent, ChargebackEvent, SettlementEvent


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC_PAYMENTS", "payments.raw")


def _delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")


def get_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 3,
    })


def produce_events(
    count: int = 1000,
    events_per_second: int = 100,
    producer: Producer | None = None,
    payment_pool: list[PaymentEvent] | None = None,
) -> list[PaymentEvent]:
    """Produce `count` mixed financial events to Kafka.

    Returns list of generated PaymentEvents so callers can build referential events.
    """
    p = producer or get_producer()
    generated_payments: list[PaymentEvent] = payment_pool or []

    interval = 1.0 / events_per_second

    for i in range(count):
        roll = random.random()
        if roll < 0.80 or not generated_payments:
            event = PaymentEvent.generate()
            generated_payments.append(event)
        elif roll < 0.90 and generated_payments:
            ref = random.choice(generated_payments)
            event = RefundEvent.generate(ref.transaction_id, ref.merchant_id, ref.amount)
        elif roll < 0.92 and generated_payments:
            ref = random.choice(generated_payments)
            event = ChargebackEvent.generate(ref.transaction_id, ref.merchant_id, ref.amount)
        else:
            ref = random.choice(generated_payments) if generated_payments else PaymentEvent.generate()
            event = SettlementEvent.generate(ref.merchant_id, ref.amount * random.uniform(5, 20))

        p.produce(TOPIC, json.dumps(asdict(event)).encode(), callback=_delivery_report)

        if i % 500 == 0:
            p.poll(0)
        time.sleep(interval)

    p.flush()
    return generated_payments
