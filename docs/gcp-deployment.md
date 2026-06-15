# GCP Deployment Guide

**Audience**: First-time GCP users deploying the batch data pipeline.

**What you get**: The full pipeline stack (Spark, Kafka, Delta Lake, Grafana, Kibana, Jaeger) running on a single GCE virtual machine, with Google Cloud Storage (GCS) as the data lake instead of the local fake-gcs-server emulator.

---

## Table of Contents

1. [Architecture on GCP](#1-architecture-on-gcp)
2. [Prerequisites](#2-prerequisites)
3. [Phase 1 — GCP Project Setup](#3-phase-1--gcp-project-setup)
4. [Phase 2 — Create GCS Bucket (Data Lake)](#4-phase-2--create-gcs-bucket-data-lake)
5. [Phase 3 — Service Account & Permissions](#5-phase-3--service-account--permissions)
6. [Phase 4 — Create the GCE Virtual Machine](#6-phase-4--create-the-gce-virtual-machine)
7. [Phase 5 — Deploy the Stack on the VM](#7-phase-5--deploy-the-stack-on-the-vm)
8. [Phase 6 — Run & Test the Pipeline](#8-phase-6--run--test-the-pipeline)
9. [Phase 7 — Monitor](#9-phase-7--monitor)
10. [Firewall Rules Reference](#10-firewall-rules-reference)
11. [Troubleshooting](#11-troubleshooting)
12. [Cost Estimation](#12-cost-estimation)
13. [Cleanup](#13-cleanup)

---

## 1. Architecture on GCP

```
┌─────────────────────────── GCE Virtual Machine (e2-standard-8) ─────────────────┐
│                                                                                   │
│  ┌─────────────────┐  ┌─────────────────────────────┐  ┌────────────────────┐   │
│  │  Apache Kafka   │  │  Spark Standalone Cluster    │  │   Observability    │   │
│  │  + Zookeeper    │  │  spark-master                │  │  Prometheus        │   │
│  │  + Kafka UI     │  │  spark-worker-1              │  │  Grafana           │   │
│  └────────┬────────┘  │  spark-worker-2              │  │  Jaeger            │   │
│           │           └──────────────┬───────────────┘  │  Elasticsearch     │   │
│           │ events                   │ Delta read/write  │  Kibana            │   │
│           ▼                         ▼                   └────────────────────┘   │
│  ┌─────────────────┐       ┌──────────────────┐                                  │
│  │  fluxlake-api   │──────▶│   Delta Utils    │                                  │
│  │  (FastAPI)      │       │  gs:// scheme    │                                  │
│  └─────────────────┘       └────────┬─────────┘                                  │
│                                     │                                             │
└─────────────────────────────────────┼─────────────────────────────────────────────┘
                                      │  GCS Hadoop Connector
                                      ▼
                   ┌─────────────────────────────────────┐
                   │  Google Cloud Storage (GCS) Bucket  │
                   │  gs://your-pipeline-datalake/       │
                   │    bronze/   silver/   gold/        │
                   │    checkpoints/   spark-events/     │
                   └─────────────────────────────────────┘
```

**Key difference from local dev**: The local `fake-gcs-server` emulator is removed. All Delta Lake tables are written to your real GCS bucket using the same `gs://` URI scheme and Hadoop GCS Connector as in local dev — only `GCS_EMULATOR_HOST` is unset and service account credentials are injected.

---

## 2. Prerequisites

You need the following on your **local machine** (the machine you SSH from, not the GCP VM):

| Tool | Install |
|------|---------|
| `gcloud` CLI | [cloud.google.com/sdk/docs/install](https://cloud.google.com/sdk/docs/install) |
| `git` | Already installed on most systems |
| SSH key pair | `ssh-keygen -t ed25519` if you don't have one |

### Verify gcloud is installed

```bash
gcloud version
# Expected: Google Cloud SDK 4xx.x.x
```

### Log in to GCP

```bash
gcloud auth login
# A browser window opens — sign in with your Google account
```

---

## 3. Phase 1 — GCP Project Setup

### Step 1.1 — Create (or select) a GCP Project

```bash
# Create a new project
gcloud projects create my-fluxlake-gcp --name="FluxLakeGCP"

# Set it as the active project for all gcloud commands
gcloud config set project my-fluxlake-gcp
```

> **What is a project?** A GCP project is a container for all your cloud resources (VMs, buckets, IAM, billing). Think of it like a folder that groups everything for one application.

### Step 1.2 — Enable Billing

GCP requires a billing account to create VMs.

1. Go to [console.cloud.google.com/billing](https://console.cloud.google.com/billing)
2. Click **"Link a billing account"** → follow prompts to add a credit card.
3. New accounts get **$300 free credits** valid for 90 days.

### Step 1.3 — Enable Required APIs

```bash
gcloud services enable \
  compute.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com
```

> These commands allow GCP to: create VMs (`compute`), use Cloud Storage (`storage`), and manage permissions (`iam`).

---

## 4. Phase 2 — Create GCS Bucket (Data Lake)

GCS replaces the local `fake-gcs-server` emulator. All Delta Lake tables live here.

### Step 4.1 — Choose a globally unique bucket name

Bucket names are global across all of GCP. Use a name that includes your project ID to guarantee uniqueness.

```bash
# Save the name in a shell variable — you'll reuse it throughout this guide
export GCS_BUCKET="my-fluxlake-gcp-datalake"
export GCP_REGION="us-central1"
```

### Step 4.2 — Create the bucket

```bash
gsutil mb -l ${GCP_REGION} gs://${GCS_BUCKET}
```

### Step 4.3 — Create the folder structure inside the bucket

```bash
# GCS doesn't have real folders, but creating placeholder objects makes the
# structure visible in the console and satisfies some tools.
gsutil cp /dev/null gs://${GCS_BUCKET}/bronze/.keep
gsutil cp /dev/null gs://${GCS_BUCKET}/silver/.keep
gsutil cp /dev/null gs://${GCS_BUCKET}/gold/.keep
gsutil cp /dev/null gs://${GCS_BUCKET}/checkpoints/.keep
gsutil cp /dev/null gs://${GCS_BUCKET}/spark-events/.keep
```

### Step 4.4 — Verify

```bash
gsutil ls gs://${GCS_BUCKET}/
# Expected output:
# gs://my-fluxlake-gcp-datalake/bronze/
# gs://my-fluxlake-gcp-datalake/checkpoints/
# gs://my-fluxlake-gcp-datalake/gold/
# gs://my-fluxlake-gcp-datalake/silver/
# gs://my-fluxlake-gcp-datalake/spark-events/
```

---

## 5. Phase 3 — Service Account & Permissions

A **service account** is a special GCP identity for applications (not humans) to authenticate. The pipeline containers will use this identity to read/write GCS.

### Step 5.1 — Create the service account

```bash
gcloud iam service-accounts create pipeline-sa \
  --display-name="Pipeline Service Account"
```

### Step 5.2 — Grant GCS access to the service account

```bash
export GCP_PROJECT_ID=$(gcloud config get-value project)

# Storage Admin = full read/write/delete on GCS objects
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} \
  --member="serviceAccount:pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

### Step 5.3 — Download the service account key

```bash
gcloud iam service-accounts keys create ~/gcp-sa-key.json \
  --iam-account="pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
```

> **Security note**: This key file grants full GCS access. Do not commit it to git. It is listed in `.gitignore`. Store it securely.

---

## 6. Phase 4 — Create the GCE Virtual Machine

### Step 6.1 — Create the VM

The pipeline requires significant RAM for Spark. `e2-standard-8` (8 vCPUs, 32 GB RAM) is a good starting point.

```bash
gcloud compute instances create pipeline-vm \
  --zone=${GCP_REGION}-a \
  --machine-type=e2-standard-8 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=100GB \
  --boot-disk-type=pd-ssd \
  --tags=pipeline-server \
  --metadata=enable-oslogin=true
```

> **What does each flag mean?**
> - `machine-type=e2-standard-8` — 8 vCPUs, 32 GB RAM (~$0.27/hour)
> - `image-family=ubuntu-2204-lts` — Ubuntu 22.04 LTS operating system
> - `boot-disk-size=100GB` — disk for Docker images and local temp files
> - `tags=pipeline-server` — network tag used to apply firewall rules later

### Step 6.2 — Get the VM's external IP

```bash
gcloud compute instances describe pipeline-vm \
  --zone=${GCP_REGION}-a \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
# Example output: 34.72.183.45
```

Save this IP — you'll use it to access the web UIs.

```bash
export GCE_EXTERNAL_IP="34.72.183.45"   # replace with your actual IP
```

### Step 6.3 — Open firewall ports

```bash
gcloud compute firewall-rules create pipeline-ports \
  --allow=tcp:8000,tcp:8080,tcp:8081,tcp:8082,tcp:8090,tcp:3000,tcp:5601,tcp:9090,tcp:16686,tcp:4317,tcp:4318,tcp:29092 \
  --target-tags=pipeline-server \
  --description="Pipeline stack ports"
```

> These ports expose: FluxLake API (8000), Spark UI (8080-8082), Kafka UI (8090), Grafana (3000), Kibana (5601), Prometheus (9090), Jaeger (16686), OTel (4317/4318), Kafka external (29092).

### Step 6.4 — SSH into the VM

```bash
gcloud compute ssh pipeline-vm --zone=${GCP_REGION}-a
# You are now inside the VM's terminal
```

### Step 6.5 — Install Docker and Docker Compose on the VM

Run these commands **inside the VM**:

```bash
# Update package list
sudo apt-get update

# Install Docker
sudo apt-get install -y docker.io docker-compose-plugin curl git make

# Add your user to the docker group (so you don't need sudo for docker)
sudo usermod -aG docker $USER

# Apply the group change without logging out
newgrp docker

# Verify Docker works
docker run --rm hello-world
# Expected: "Hello from Docker!"
```

---

## 7. Phase 5 — Deploy the Stack on the VM

All remaining steps run **inside the VM** (via SSH).

### Step 7.1 — Clone the repository

```bash
git clone https://github.com/amudhan023/flux-lake-gcp.git
cd flux-lake-gcp
```

### Step 7.2 — Copy the service account key to the VM

**On your local machine** (open a new terminal, not the SSH session):

```bash
gcloud compute scp ~/gcp-sa-key.json pipeline-vm:~/gcp-sa-key.json \
  --zone=us-central1-a
```

Switch back to the SSH terminal on the VM.

### Step 7.3 — Create your `.env.gcp` file

```bash
cp .env.gcp.example .env.gcp
nano .env.gcp   # or: vi .env.gcp
```

Edit the following values (replace placeholders with your real values):

```bash
GCP_PROJECT_ID=my-fluxlake-gcp          # your GCP project ID
GCP_REGION=us-central1
GCS_BUCKET=my-fluxlake-gcp-datalake     # the bucket you created in Phase 2
GCP_SA_KEY_PATH=/home/YOUR_USERNAME/gcp-sa-key.json   # path to your key file
GCE_EXTERNAL_IP=34.72.183.45               # your VM's external IP from Step 6.2
GRAFANA_ADMIN_PASSWORD=ChangeMe123!        # set a strong password
CHECKPOINT_BASE=gs://my-fluxlake-gcp-datalake/checkpoints
```

Save the file (in nano: `Ctrl+O`, `Enter`, `Ctrl+X`).

### Step 7.4 — Start the stack

```bash
make gcp-up
```

This command runs `docker compose -f docker-compose.yml -f docker-compose.gcp.yml up -d`.

The first run pulls ~6 GB of Docker images. Expect 5–10 minutes on a fresh VM.

### Step 7.5 — Verify all containers are running

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Expected output (all should show `Up`):

```
NAMES                STATUS
spark-master         Up 2 minutes (healthy)
spark-worker-1       Up 2 minutes
spark-worker-2       Up 2 minutes
kafka                Up 2 minutes (healthy)
zookeeper            Up 2 minutes (healthy)
kafka-ui             Up 2 minutes
fluxlake-api         Up 2 minutes
prometheus           Up 2 minutes
grafana              Up 2 minutes
otel-collector       Up 2 minutes
jaeger               Up 2 minutes
elasticsearch        Up 2 minutes (healthy)
logstash             Up 2 minutes
kibana               Up 2 minutes
filebeat             Up 2 minutes
```

### Step 7.6 — Verify the FluxLake API is healthy

```bash
curl http://localhost:8000/health
# Expected: {"status":"ok"}
```

### Step 7.7 — Verify GCS connectivity from Spark

```bash
docker exec spark-master bash -c "
  python3 -c \"
import subprocess
result = subprocess.run(
    ['gsutil', 'ls', 'gs://\${GCS_BUCKET}/'],
    capture_output=True, text=True
)
print(result.stdout or result.stderr)
\"
"
```

If you see the bucket contents, GCS access is working.

---

## 8. Phase 6 — Run & Test the Pipeline

### Step 8.1 — Seed historical Bronze data

This generates 90 days of synthetic payment/refund/chargeback/settlement events and writes them to GCS Bronze tables.

```bash
make gcp-seed
# Takes 5–10 minutes. Watch progress:
docker logs -f fluxlake-api
```

### Step 8.2 — Verify data landed in GCS

```bash
gsutil ls gs://${GCS_BUCKET}/bronze/
# Expected:
# gs://my-fluxlake-gcp-datalake/bronze/raw_payments/
# gs://my-fluxlake-gcp-datalake/bronze/raw_refunds/
# gs://my-fluxlake-gcp-datalake/bronze/raw_chargebacks/
# gs://my-fluxlake-gcp-datalake/bronze/raw_settlements/
# gs://my-fluxlake-gcp-datalake/bronze/dead_letter/
```

Check the size of the Bronze layer:

```bash
gsutil du -sh gs://${GCS_BUCKET}/bronze/
# Example: 450 MiB
```

### Step 8.3 — Trigger the full pipeline (Bronze → Silver → Gold)

```bash
make gcp-run-pipeline
# Expected JSON response:
# {"message": "Pipeline triggered", "run_id": "run_20260614_120000_abc123"}
```

Check pipeline status:

```bash
curl http://localhost:8000/status
# Possible values: {"status": "running"} or {"status": "complete", "duration_s": 234.5}
```

Tail logs while it runs:

```bash
docker logs -f fluxlake-api
```

### Step 8.4 — Verify Silver and Gold data in GCS

After the pipeline completes (~5–15 minutes depending on VM size):

```bash
gsutil ls gs://${GCS_BUCKET}/silver/
# gs://...datalake/silver/cleansed_transactions/
# gs://...datalake/silver/reconciliation_ledger/
# gs://...datalake/silver/dispute_registry/

gsutil ls gs://${GCS_BUCKET}/gold/
# gs://...datalake/gold/daily_merchant_summary/
# gs://...datalake/gold/hourly_transaction_volume/
# gs://...datalake/gold/reconciliation_report/
# gs://...datalake/gold/ml_feature_store/
```

### Step 8.5 — Produce live Kafka events (optional)

To test the streaming Bronze ingestion path:

```bash
docker exec fluxlake-api python /app/scripts/run_pipeline.py
```

Or produce events via the Kafka producer:

```bash
docker exec fluxlake-api python -c "
from src.python.ingestion.kafka_producer import produce_events
produce_events(count=500, events_per_second=50)
print('Done')
"
```

### Step 8.6 — Run the test suite

```bash
# Unit tests (no infrastructure needed):
make test-unit

# Integration tests (requires the running stack):
make test-integration
```

Expected: all tests pass. Integration tests read/write against GCS via the running Spark cluster.

---

## 9. Phase 7 — Monitor

All monitoring UIs are accessible via the VM's external IP. Replace `<VM-IP>` with your actual external IP (`echo $GCE_EXTERNAL_IP`).

### Grafana — Dashboards

URL: `http://<VM-IP>:3000`
Login: admin / (the password you set in .env.gcp)

Six pre-built dashboards are provisioned automatically:

| Dashboard | What it shows |
|-----------|--------------|
| **Pipeline Overview** | Run count, duration, records per stage |
| **Spark Performance** | Executor memory, shuffle, task parallelism |
| **Delta Lake Metrics** | OPTIMIZE runs, Z-ORDER gains, VACUUM |
| **Kafka Topics** | Consumer lag, messages/s, partition distribution |
| **Data Quality** | Null rates, duplicate rates, quarantine counts |
| **SLA Tracking** | Stage durations vs. SLA targets, trend over time |

Click `Explore` → `Prometheus` to run ad-hoc PromQL queries.

### Spark UI — Job Execution

URL: `http://<VM-IP>:8080`

- Click any **Application** link to see the Spark UI for a running job.
- **Stages** tab: identify slow stages.
- **SQL** tab: see physical query plans.
- **Executors** tab: check memory and GC pressure.

### Jaeger — Distributed Traces

URL: `http://<VM-IP>:16686`

1. Select **Service**: `fluxlake-api`
2. Click **Find Traces**
3. Click any trace to see the waterfall: `bronze_ingestion → kafka_consume → delta_write → silver_cleanse → ...`

This shows exactly where latency comes from across pipeline stages.

### Kibana — Structured Logs

URL: `http://<VM-IP>:5601`

First-time setup:
1. Go to **Stack Management** → **Index Patterns**
2. Create pattern: `logstash-*`
3. Set time field: `@timestamp`
4. Go to **Discover** to search logs

Useful queries:
- `run_id: run_20260614*` — all logs for a specific run
- `level: ERROR` — error events only
- `stage: silver_cleanse` — logs from a specific stage

### Prometheus — Raw Metrics

URL: `http://<VM-IP>:9090`

Useful queries in the **Graph** tab:

```promql
# Records processed per stage
pipeline_records_processed_total

# Schema error rate
rate(pipeline_schema_errors_total[5m])

# FluxLake API request latency
histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))
```

### GCS Console — Data Lake Browser

In a browser: [console.cloud.google.com/storage/browser](https://console.cloud.google.com/storage/browser)

Select your bucket to browse Delta table files, view partition structure, and check object sizes.

---

## 10. Firewall Rules Reference

| Port | Service | Who needs access |
|------|---------|-----------------|
| 8000 | FluxLake API | Your IP or team |
| 8080 | Spark Master UI | Your IP |
| 8081-8082 | Spark Worker UIs | Your IP |
| 8090 | Kafka UI | Your IP |
| 3000 | Grafana | Your IP or team |
| 5601 | Kibana | Your IP |
| 9090 | Prometheus | Your IP |
| 16686 | Jaeger | Your IP |
| 29092 | Kafka (external) | Kafka producers only |

**Restrict access to your IP only** (recommended):

```bash
# Find your public IP
MY_IP=$(curl -s ifconfig.me)

# Create a tighter rule
gcloud compute firewall-rules create pipeline-myip \
  --allow=tcp:8000,tcp:8080,tcp:3000,tcp:5601,tcp:9090,tcp:16686 \
  --source-ranges=${MY_IP}/32 \
  --target-tags=pipeline-server
```

---

## 11. Troubleshooting

### Container won't start

```bash
# Check logs for the specific container
docker logs spark-master
docker logs fluxlake-api
docker logs kafka
```

### GCS permission denied

```bash
# Test the SA key directly
GOOGLE_APPLICATION_CREDENTIALS=~/gcp-sa-key.json gsutil ls gs://${GCS_BUCKET}/
```

If this fails, the key doesn't have the right permissions. Re-run Step 5.2.

### Spark can't write to GCS

Symptom: `com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem` class not found.

The GCS connector JAR is downloaded at Spark startup via `spark.jars.packages`. If the VM has no internet access from inside containers, you need to pre-download the JAR.

```bash
# Inside the spark-master container:
docker exec spark-master bash -c "
  cd /tmp && \
  curl -LO https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar && \
  cp gcs-connector-hadoop3-latest.jar /opt/bitnami/spark/jars/
"
docker restart spark-master spark-worker-1 spark-worker-2
```

### Checkpoint directory error

```bash
# Verify the checkpoints path exists in GCS
gsutil ls gs://${GCS_BUCKET}/checkpoints/
# If missing:
gsutil cp /dev/null gs://${GCS_BUCKET}/checkpoints/.keep
```

### FluxLake API returns 500

```bash
docker logs fluxlake-api --tail=50
```

Look for: `Connection refused` (Spark not ready), `NoSuchBucketException` (wrong bucket name), `AccessDeniedException` (SA key issue).

### Kafka producer can't connect externally

Make sure `GCE_EXTERNAL_IP` in `.env.gcp` matches the VM's actual external IP, and that port 29092 is open in the firewall rule.

### Out of disk space

```bash
df -h
# If /dev/sda is > 85% full:
docker system prune -f   # remove unused images/containers
```

### Elasticsearch won't start

Elasticsearch needs `vm.max_map_count` set:

```bash
sudo sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
```

---

## 12. Cost Estimation

Costs are approximate (us-central1, June 2026).

| Resource | Spec | Cost/hour | Cost/month |
|----------|------|-----------|------------|
| GCE VM | e2-standard-8 (8 vCPU, 32 GB) | ~$0.27 | ~$194 |
| Boot disk | 100 GB SSD | ~$0.001 | ~$1.70 |
| GCS storage | 100 GB data lake | — | ~$2.30 |
| GCS operations | ~1M reads + 100k writes | — | ~$0.05 |
| Network egress | Minimal (same region) | — | ~$0 |
| **Total** | | | **~$200/month** |

**To reduce costs:**
- Stop the VM when not in use: `gcloud compute instances stop pipeline-vm --zone=us-central1-a`
- Use a preemptible/spot VM (70% cheaper, can be interrupted): add `--provisioning-model=SPOT` to the create command
- Use `e2-standard-4` (4 vCPU, 16 GB) for lighter testing (~$97/month)

---

## 13. Cleanup

To avoid ongoing charges, delete all resources when done.

```bash
# Step 1 — Delete the VM (stops billing for compute immediately)
gcloud compute instances delete pipeline-vm --zone=us-central1-a --quiet

# Step 2 — Delete the GCS bucket and all data
gsutil rm -r gs://${GCS_BUCKET}

# Step 3 — Delete the firewall rule
gcloud compute firewall-rules delete pipeline-ports --quiet

# Step 4 — Delete the service account key (optional — deletes the local file)
rm ~/gcp-sa-key.json

# Step 5 — Delete the service account
gcloud iam service-accounts delete \
  "pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" --quiet

# Step 6 — Delete the project entirely (removes ALL resources and billing)
# Only do this if you no longer need the project at all.
gcloud projects delete ${GCP_PROJECT_ID} --quiet
```

---

## Quick Reference Card

```
# On your local machine:
gcloud compute ssh pipeline-vm --zone=us-central1-a

# On the VM:
cd flux-lake-gcp
make gcp-up                         # start all services
make gcp-seed                       # generate 90 days of test data
make gcp-run-pipeline               # trigger Bronze→Silver→Gold
curl http://localhost:8000/status   # check pipeline status
make gcp-logs                       # tail all logs
make gcp-down                       # stop all services

# Access UIs (replace <VM-IP>):
# Grafana:  http://<VM-IP>:3000
# Spark:    http://<VM-IP>:8080
# Jaeger:   http://<VM-IP>:16686
# Kibana:   http://<VM-IP>:5601
# Kafka UI: http://<VM-IP>:8090
```
