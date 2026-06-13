"""Peak traffic scenario: ramp 10k→100k events/min, hold 20min, ramp down."""
import random
from dataclasses import asdict
from locust import HttpUser, task, between, constant_pacing
import sys
sys.path.insert(0, "/app")

from src.python.ingestion.event_types import PaymentEvent, SettlementEvent

_pool: list[PaymentEvent] = []


class PeakTrafficUser(HttpUser):
    wait_time = constant_pacing(0.001)  # 1ms between tasks = 1000 req/s per user

    @task(9)
    def payment(self):
        event = PaymentEvent.generate()
        _pool.append(event)
        if len(_pool) > 50_000:
            _pool.pop(0)
        self.client.post("/ingest", json=asdict(event), name="/ingest [peak:payment]")

    @task(1)
    def settlement(self):
        if not _pool:
            return
        ref = random.choice(_pool)
        event = SettlementEvent.generate(ref.merchant_id, ref.amount * 10)
        self.client.post("/ingest", json=asdict(event), name="/ingest [peak:settlement]")
