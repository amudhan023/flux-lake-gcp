"""Normal daily scenario: 10k events/min for 60 minutes."""
import json
import random
from dataclasses import asdict
from locust import HttpUser, task, between
import sys
sys.path.insert(0, "/app")

from src.python.ingestion.event_types import (
    PaymentEvent, RefundEvent, ChargebackEvent, SettlementEvent
)

_payment_pool: list[PaymentEvent] = []


class NormalDailyUser(HttpUser):
    wait_time = between(0.005, 0.01)  # ~100 req/s per user at 50 users → 5k/s

    @task(80)
    def produce_payment(self):
        event = PaymentEvent.generate()
        _payment_pool.append(event)
        if len(_payment_pool) > 10_000:
            _payment_pool.pop(0)
        self.client.post(
            "/ingest",
            json=asdict(event),
            name="/ingest [payment]",
        )

    @task(10)
    def produce_refund(self):
        if not _payment_pool:
            return
        ref = random.choice(_payment_pool)
        event = RefundEvent.generate(ref.transaction_id, ref.merchant_id, ref.amount)
        self.client.post("/ingest", json=asdict(event), name="/ingest [refund]")

    @task(2)
    def produce_chargeback(self):
        if not _payment_pool:
            return
        ref = random.choice(_payment_pool)
        event = ChargebackEvent.generate(ref.transaction_id, ref.merchant_id, ref.amount)
        self.client.post("/ingest", json=asdict(event), name="/ingest [chargeback]")

    @task(8)
    def produce_settlement(self):
        if not _payment_pool:
            return
        ref = random.choice(_payment_pool)
        event = SettlementEvent.generate(ref.merchant_id, ref.amount * random.uniform(5, 20))
        self.client.post("/ingest", json=asdict(event), name="/ingest [settlement]")

    @task(1)
    def check_status(self):
        self.client.get("/status", name="/status")
