.PHONY: up down infra-up infra-down seed run-pipeline load-test test test-unit \
        test-integration test-dq benchmark logs grafana kibana jaeger help

include .env.example
-include .env
export

COMPOSE      = docker compose -f docker-compose.yml
COMPOSE_INFRA = docker compose -f docker-compose.infra.yml

GRAFANA_URL  = http://localhost:3000
KIBANA_URL   = http://localhost:5601
JAEGER_URL   = http://localhost:16686

help:
	@echo ""
	@echo "  Pipeline Project — Make Targets"
	@echo "  ────────────────────────────────────────────────────"
	@echo "  make up               Start full stack"
	@echo "  make down             Tear down full stack"
	@echo "  make infra-up         Start infra only (Spark, Kafka, MinIO, obs.)"
	@echo "  make infra-down       Tear down infra"
	@echo "  make seed             Seed 90 days of historical Bronze data"
	@echo "  make run-pipeline     Trigger full Bronze→Silver→Gold pipeline"
	@echo "  make load-test        Run all 4 Locust load test scenarios"
	@echo "  make test             Full test suite (unit + integration + dq)"
	@echo "  make test-unit        Unit tests only (~30s, no Docker)"
	@echo "  make test-integration Integration tests (~5m, needs infra-up)"
	@echo "  make test-dq          Great Expectations data quality suite"
	@echo "  make benchmark        SLA comparison benchmark (~15m)"
	@echo "  make logs             Tail all container logs"
	@echo "  make grafana          Open Grafana in browser"
	@echo "  make kibana           Open Kibana in browser"
	@echo "  make jaeger           Open Jaeger in browser"
	@echo ""

up:
	@echo "Starting full stack..."
	$(COMPOSE) up -d --scale spark-worker-1=0 --scale spark-worker-2=0
	$(COMPOSE) up -d \
		--scale spark-worker-1=$(or $(SPARK_WORKERS),2) \
		2>/dev/null || true
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@echo ""
	@echo "Stack is up. Service URLs:"
	@echo "  Spark UI:      http://localhost:8080"
	@echo "  MinIO Console: http://localhost:9001"
	@echo "  Kafka UI:      http://localhost:8090"
	@echo "  Grafana:       $(GRAFANA_URL)  (admin / admin123)"
	@echo "  Jaeger:        $(JAEGER_URL)"
	@echo "  Kibana:        $(KIBANA_URL)"
	@echo "  Pipeline API:  http://localhost:8000"

down:
	$(COMPOSE) down -v

infra-up:
	@echo "Starting infrastructure stack..."
	$(COMPOSE_INFRA) up -d
	@echo "Infrastructure up. Spark Master: http://localhost:8080"

infra-down:
	$(COMPOSE_INFRA) down -v

seed:
	@echo "Seeding 90 days of historical Bronze data..."
	docker exec pipeline-api python /app/scripts/seed_data.py
	@echo "Seed complete."

run-pipeline:
	@echo "Triggering full pipeline run..."
	curl -s -X POST http://localhost:8000/trigger | python3 -m json.tool
	@echo ""

load-test:
	@echo "Running all Locust load test scenarios..."
	@echo "Watch live: $(GRAFANA_URL)/d/pipeline-overview"
	cd tests/load && locust -f locustfile.py --headless \
		-u $(or $(LOAD_TEST_USERS),50) \
		-r $(or $(LOAD_TEST_SPAWN_RATE),5) \
		--run-time 5m \
		--host http://localhost:8000
	@echo "Load test complete. Check $(GRAFANA_URL)"

test: test-unit test-integration test-dq
	@echo "All tests passed."

test-unit:
	@echo "Running unit tests..."
	python -m pytest tests/unit/ -v --tb=short \
		--junitxml=tests/reports/unit.xml \
		--cov=src/python \
		--cov-report=term-missing \
		--cov-fail-under=80
	@echo "Unit tests complete."

test-integration:
	@echo "Running integration tests (requires infra-up)..."
	python -m pytest tests/integration/ -v --tb=short \
		--junitxml=tests/reports/integration.xml \
		-m integration
	@echo "Integration tests complete."

test-dq:
	@echo "Running Great Expectations data quality suite..."
	python -m pytest tests/data_quality/ -v --tb=short \
		--junitxml=tests/reports/dq.xml
	@echo "Data quality tests complete."

benchmark:
	@echo "Running SLA benchmark comparison (~15 minutes)..."
	docker exec pipeline-api python /app/scripts/benchmark_comparison.py
	@echo "Benchmark complete. Results in benchmark_results.json"
	@echo "View: $(GRAFANA_URL)/d/sla-tracking"

logs:
	$(COMPOSE) logs -f --tail=100

grafana:
	open $(GRAFANA_URL) 2>/dev/null || xdg-open $(GRAFANA_URL) 2>/dev/null || \
		echo "Open browser: $(GRAFANA_URL)"

kibana:
	open $(KIBANA_URL) 2>/dev/null || xdg-open $(KIBANA_URL) 2>/dev/null || \
		echo "Open browser: $(KIBANA_URL)"

jaeger:
	open $(JAEGER_URL) 2>/dev/null || xdg-open $(JAEGER_URL) 2>/dev/null || \
		echo "Open browser: $(JAEGER_URL)"
