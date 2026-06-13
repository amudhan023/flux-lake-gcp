"""Month-end reconciliation scenario: 5% intentional settlement discrepancies."""
import sys
import random
sys.path.insert(0, "/app")

from locust import HttpUser, task, between
from dataclasses import asdict
from src.python.ingestion.event_types import PaymentEvent, SettlementEvent

_pool: list[PaymentEvent] = []


class ReconciliationUser(HttpUser):
    wait_time = between(0.01, 0.05)

    @task(8)
    def payment(self):
        event = PaymentEvent.generate()
        _pool.append(event)
        self.client.post("/ingest", json=asdict(event), name="/ingest [recon:payment]")

    @task(2)
    def settlement(self):
        if not _pool:
            return
        ref = random.choice(_pool)
        settlement = SettlementEvent.generate(ref.merchant_id, ref.amount * 10)

        if random.random() < 0.05:
            # Intentional discrepancy: reduce settled amount by 3–15%
            settlement.gross_amount *= random.uniform(0.85, 0.97)
            settlement.net_amount = round(settlement.gross_amount - settlement.fees, 2)

        self.client.post("/ingest", json=asdict(settlement), name="/ingest [recon:settlement]")
