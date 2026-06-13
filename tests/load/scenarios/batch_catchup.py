"""Batch catchup scenario: generate 30 days of historical events (~50M total)."""
import sys
import os
sys.path.insert(0, "/app")

from locust import HttpUser, task, between
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import random

from src.python.ingestion.event_types import PaymentEvent, RefundEvent, SettlementEvent


class BatchCatchupUser(HttpUser):
    wait_time = between(0.0001, 0.001)
    _day_offset = 30

    @task
    def catchup_event(self):
        target_date = datetime.now(timezone.utc) - timedelta(days=self.__class__._day_offset)
        event = PaymentEvent.generate()
        event.timestamp = target_date.replace(
            hour=random.randint(0, 23), minute=random.randint(0, 59)
        ).isoformat() + "Z"

        self.client.post("/ingest", json=asdict(event), name=f"/ingest [catchup:day-{self.__class__._day_offset}]")

        if random.random() < 0.01:
            self.__class__._day_offset = max(1, self.__class__._day_offset - 1)
