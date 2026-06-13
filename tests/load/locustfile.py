"""Locust orchestrator — imports all scenario tasks."""
import os
from locust import HttpUser, between, events
from scenarios.normal_daily import NormalDailyUser
from scenarios.peak_traffic import PeakTrafficUser

# Default: run normal daily scenario. Override with LOAD_SCENARIO env var.
SCENARIO = os.getenv("LOAD_SCENARIO", "normal")


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    print(f"Load scenario: {SCENARIO}")
    print(f"Grafana: http://localhost:3000/d/pipeline-overview")
