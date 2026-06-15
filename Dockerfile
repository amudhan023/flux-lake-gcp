FROM bitnami/spark:3.5

USER root

RUN apt-get update && apt-get install -y python3-pip curl && \
    pip3 install --no-cache-dir \
        delta-spark==3.2.0 \
        pyspark==3.5.0 \
        kafka-python==2.0.2 \
        confluent-kafka==2.3.0 \
        prometheus-client==0.19.0 \
        opentelemetry-api==1.22.0 \
        opentelemetry-sdk==1.22.0 \
        opentelemetry-exporter-otlp==1.22.0 \
        fastapi==0.109.0 \
        uvicorn==0.27.0 \
        great-expectations==0.18.8 \
        chispa==0.9.4 \
        pytest==7.4.4 \
        pytest-cov==4.1.0 \
        pytest-mock==3.12.0 \
        locust==2.20.0 \
        faker==22.0.0 \
        python-dotenv==1.0.0

WORKDIR /app

COPY src/ /app/src/
COPY scripts/ /app/scripts/
COPY config/ /app/config/

CMD ["uvicorn", "src.python.fluxlake_api:app", "--host", "0.0.0.0", "--port", "8000"]
