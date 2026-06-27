# 📊 Mobile Brands End-to-End Data Pipeline — Deployment Guide

> **Who is this for?** Any engineer who needs to deploy this pipeline from scratch on GCP.
> Follow the steps top-to-bottom. Every command includes a comment explaining what it does.

---

## 🗺️ Pipeline Overview

```
Cloud Run: Ingestion  →  Cloud Run: Transform  →  Dataproc: Bronze → Silver → Gold  →  Cloud Run: Archival  →  BigQuery
```

Orchestrated by **Cloud Composer (Airflow)** — runs daily at 01:00 UTC.

---

## 📁 Repo Structure

```
global-mobile-brands/
├── ingestion/          # Cloud Run Job — generates Excel files → GCS landing/
│   ├── main.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── config/config.json
├── transform/          # Cloud Run Job — Excel → CSV per brand → GCS transformed/
│   ├── main.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── config/config.json
├── archival/           # Cloud Run Job — moves files to archive_landing/ & archive_transformed/
│   ├── main.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── config/config.json
├── dataproc_jobs/      # PySpark jobs uploaded to GCS and run by Dataproc
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
├── airflow/            # Airflow DAG synced to Cloud Composer
│   └── dag_end_to_end_pipeline.py
└── cloudbuild.yaml     # CI/CD — builds images, deploys jobs, syncs scripts & DAG
```

---

## ⚙️ Environment Variables Reference

| Variable | DV Value | PROD Value |
|---|---|---|
| `ENV` | `dv` | `prod` |
| `PROJECT_ID` | `dv-env` | `prod-env` |
| `REGION` | `us-central1` | `us-central1` |
| `SOURCE_BUCKET` | `dv-mb-data-bucket` | `prod-mb-data-bucket` |
| `PIPELINE_BUCKET` | `dv-mb-pipeline-bucket` | `prod-mb-pipeline-bucket` |
| `REGISTRY` | `us-central1-docker.pkg.dev/dv-env/mb-repo` | `us-central1-docker.pkg.dev/prod-env/mb-repo` |
| `SA_EMAIL` | `mb-pipeline-sa@dv-env.iam.gserviceaccount.com` | `mb-pipeline-sa@prod-env.iam.gserviceaccount.com` |

> 💡 Run all Cloud Shell commands from the **repo root** (`global-mobile-brands/`).

---

## 🔧 STEP 0 — One-time GCP Setup

Run these once per project (DV or PROD). Replace `PROJECT_ID` with your actual project.

```bash
# ── Set your project ──────────────────────────────────────────────────────────
export PROJECT_ID="dv-env"           # change to prod-env for PROD
export ENV="dv"                      # change to prod for PROD
export REGION="us-central1"
export SOURCE_BUCKET="dv-mb-data-bucket"
export PIPELINE_BUCKET="dv-mb-pipeline-bucket"
export SA_EMAIL="mb-pipeline-sa@${PROJECT_ID}.iam.gserviceaccount.com"
export REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/mb-repo"

# Set the active project
gcloud config set project $PROJECT_ID

# ── Enable required APIs ──────────────────────────────────────────────────────
gcloud services enable \
  run.googleapis.com \
  dataproc.googleapis.com \
  composer.googleapis.com \
  container.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com

# ── Create Artifact Registry repository for Docker images ─────────────────────
gcloud artifacts repositories create mb-repo \
  --repository-format=docker \
  --location=$REGION \
  --description="Mobile Brands pipeline images"

# ── Create service account ────────────────────────────────────────────────────
gcloud iam service-accounts create mb-pipeline-sa \
  --display-name="Mobile Brands Pipeline SA"

# ── Grant required roles to the service account ───────────────────────────────
for ROLE in \
  roles/storage.objectAdmin \
  roles/dataproc.editor \
  roles/dataproc.worker \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/bigquery.readSessionUser \
  roles/run.developer \
  roles/composer.worker \
  roles/iam.serviceAccountUser \
  roles/logging.logWriter; do
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="$ROLE"
done

# ── Grant Composer Service Agent permission to use the custom service account ──
export PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")

gcloud iam service-accounts add-iam-policy-binding $SA_EMAIL \
  --member="serviceAccount:service-${PROJECT_NUMBER}@cloudcomposer-accounts.iam.gserviceaccount.com" \
  --role="roles/composer.ServiceAgentV2Ext"

# ── Grant Editor role to Google APIs Service Agent ────────────────────────────
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${PROJECT_NUMBER}@cloudservices.gserviceaccount.com" \
  --role="roles/editor"

# ── Create GCS buckets ────────────────────────────────────────────────────────
# Data bucket: holds landing/, transformed/, archive_landing/, archive_transformed/
gcloud storage buckets create gs://$SOURCE_BUCKET \
  --location=$REGION \
  --uniform-bucket-level-access

# Pipeline bucket: holds scripts/ (PySpark), bronze/, silver/, gold/ (Parquet)
gcloud storage buckets create gs://$PIPELINE_BUCKET \
  --location=$REGION \
  --uniform-bucket-level-access

# ── Create BigQuery dataset ───────────────────────────────────────────────────
bq --location=$REGION mk \
  --dataset \
  --description="Mobile Brands gold layer" \
  ${PROJECT_ID}:mobile_brands
```

---

## 🐳 STEP 1 — Build & Push Docker Images

Each Cloud Run job has its own Dockerfile. Build and push all three.

```bash
# ── Configure Docker to use Artifact Registry ─────────────────────────────────
gcloud auth configure-docker ${REGION}-docker.pkg.dev

# ── Build & push: Ingestion (generates Excel files) ──────────────────────────
docker build -t ${REGISTRY}/mb-ingestion:latest ./ingestion
docker push ${REGISTRY}/mb-ingestion:latest

# ── Build & push: Transform (Excel → CSV per brand) ───────────────────────────
docker build -t ${REGISTRY}/mb-transform:latest ./transform
docker push ${REGISTRY}/mb-transform:latest

# ── Build & push: Archival (moves files to archive folders) ──────────────────
docker build -t ${REGISTRY}/mb-archival:latest ./archival
docker push ${REGISTRY}/mb-archival:latest
```

---

## ☁️ STEP 2 — Deploy Cloud Run Jobs

Create (or update) the three Cloud Run Jobs. The `--set-env-vars` sets default env vars; Airflow overrides `RUN_DATE` at execution time.

### 2a. Ingestion Job

```bash
# Create the ingestion Cloud Run Job
# This job generates Excel distributor files and uploads them to GCS landing/
gcloud run jobs deploy ${ENV}-mb-ingestion \
  --image=${REGISTRY}/mb-ingestion:latest \
  --region=$REGION \
  --project=$PROJECT_ID \
  --service-account=$SA_EMAIL \
  --set-env-vars="BUCKET_NAME=${SOURCE_BUCKET},PROJECT_ID=${PROJECT_ID}" \
  --memory=1Gi \
  --cpu=1 \
  --max-retries=3 \
  --tasks=1
```

### 2b. Transform Job

```bash
# Create the transform Cloud Run Job
# Reads Excel from landing/, splits by brand sheet, writes CSV to transformed/
gcloud run jobs deploy ${ENV}-mb-transform \
  --image=${REGISTRY}/mb-transform:latest \
  --region=$REGION \
  --project=$PROJECT_ID \
  --service-account=$SA_EMAIL \
  --set-env-vars="BUCKET_NAME=${SOURCE_BUCKET},PROJECT_ID=${PROJECT_ID}" \
  --memory=2Gi \
  --cpu=2 \
  --max-retries=3 \
  --tasks=1
```

### 2c. Archival Job

```bash
# Create the archival Cloud Run Job
# Moves processed files from landing/ & transformed/ into dated archive folders
gcloud run jobs deploy ${ENV}-mb-archive \
  --image=${REGISTRY}/mb-archival:latest \
  --region=$REGION \
  --project=$PROJECT_ID \
  --service-account=$SA_EMAIL \
  --set-env-vars="BUCKET_NAME=${SOURCE_BUCKET},PROJECT_ID=${PROJECT_ID}" \
  --memory=512Mi \
  --cpu=1 \
  --max-retries=3 \
  --tasks=1
```

### Verify all 3 jobs exist

```bash
# List all Cloud Run jobs in the project — you should see all 3
gcloud run jobs list --region=$REGION --project=$PROJECT_ID
```

---

## ⚡ STEP 3 — Upload Dataproc PySpark Scripts to GCS

The Dataproc cluster runs bronze.py, silver.py, gold.py from GCS. Upload them here.

```bash
# Create the scripts/ folder in the pipeline bucket and upload all PySpark scripts
gcloud storage cp dataproc_jobs/bronze.py gs://${PIPELINE_BUCKET}/scripts/bronze.py
gcloud storage cp dataproc_jobs/silver.py gs://${PIPELINE_BUCKET}/scripts/silver.py
gcloud storage cp dataproc_jobs/gold.py   gs://${PIPELINE_BUCKET}/scripts/gold.py

# Verify the scripts are uploaded
gcloud storage ls gs://${PIPELINE_BUCKET}/scripts/
```

---

## 🎵 STEP 4 — Deploy Airflow DAG to Cloud Composer

### 4a. Create Composer Environment (first time only)

```bash
# Create a Cloud Composer 2 environment — takes ~20-30 minutes
# Adjust --machine-type / --image-version as needed
gcloud composer environments create mb-composer \
  --location=$REGION \
  --image-version=composer-2.17.4-airflow-2.10.5 \
  --environment-size=small \
  --service-account=$SA_EMAIL

# Get the Composer GCS bucket name (you'll need this for the next steps)
export COMPOSER_BUCKET=$(gcloud composer environments describe mb-composer \
  --location=$REGION \
  --format="value(config.dagGcsPrefix)" | sed 's|gs://||' | cut -d'/' -f1)

echo "Composer bucket: $COMPOSER_BUCKET"
```

### 4b. Upload the DAG

```bash
# Copy the DAG file to the Composer dags/ folder
# Composer automatically picks up new DAG files within 1-2 minutes
gcloud storage cp airflow/dag_end_to_end_pipeline.py \
  gs://${COMPOSER_BUCKET}/dags/dag_end_to_end_pipeline.py

# Verify it was uploaded
gcloud storage ls gs://${COMPOSER_BUCKET}/dags/
```

### 4c. Set Airflow Variables

These variables are read by the DAG at runtime. Set them in the Composer environment.

```bash
# ── DV Environment variables ──────────────────────────────────────────────────
gcloud composer environments run mb-composer \
  --location=$REGION \
  variables -- set MB_ENV dv

gcloud composer environments run mb-composer \
  --location=$REGION \
  variables -- set MB_PROJECT_ID dv-env

gcloud composer environments run mb-composer \
  --location=$REGION \
  variables -- set MB_REGION us-central1

gcloud composer environments run mb-composer \
  --location=$REGION \
  variables -- set MB_SOURCE_BUCKET dv-mb-data-bucket

gcloud composer environments run mb-composer \
  --location=$REGION \
  variables -- set MB_PIPELINE_BUCKET dv-mb-pipeline-bucket

gcloud composer environments run mb-composer \
  --location=$REGION \
  variables -- set MB_BQ_DATASET mobile_brands
```

> For **PROD**: repeat the above replacing `dv` values with `prod` values in the PROD Composer environment.

### 4d. Verify Variables

```bash
# List all Airflow variables to confirm they were set correctly
gcloud composer environments run mb-composer \
  --location=$REGION \
  variables -- list
```

### 4e. Configure Airflow Email Notifications (SMTP)
To get actual emails when the DAG execution succeeds or fails, configure Gmail SMTP overrides and add the `smtp_default` connection (replace the password placeholder with your 16-character Google App Password):

1. **Add the Airflow SMTP connection**:
```bash
gcloud composer environments run mb-composer \
  --location=$REGION \
  connections add -- smtp_default \
  --conn-type=email \
  --conn-host=smtp.gmail.com \
  --conn-login=dayasagarreddy2943@gmail.com \
  --conn-password=ykqqzaokttfyqfad \
  --conn-port=587
```

2. **Apply the Airflow configuration overrides**:
```bash
gcloud composer environments update mb-composer \
  --location=$REGION \
  --update-airflow-configs=email-email_backend=airflow.utils.email.send_email_smtp,smtp-smtp_host=smtp.gmail.com,smtp-smtp_port=587,smtp-smtp_ssl=False,smtp-smtp_starttls=True,smtp-smtp_user=dayasagarreddy2943@gmail.com,smtp-smtp_mail_from=dayasagarreddy2943@gmail.com
```

---

## 🚀 STEP 5 — Trigger the Pipeline

### Manual trigger (testing)

```bash
# Trigger the DAG manually for a specific date (useful for backfill or testing)
gcloud composer environments run mb-composer \
  --location=$REGION \
  dags trigger -- dv_mobile_brands_pipeline \
  --conf '{"run_date": "2026-05-19"}'
```

### Scheduled run

The DAG runs automatically on schedule: **`0 1 * * *`** (daily at 01:00 UTC).
No action needed — Composer handles it once the DAG is deployed.

### Monitor in Airflow UI

```bash
# Get the Airflow web UI URL
gcloud composer environments describe mb-composer \
  --location=$REGION \
  --format="value(config.airflowUri)"
```

Open the URL → find `dv_mobile_brands_pipeline` → monitor task status.

---

## 🔄 STEP 6 — CI/CD via Cloud Build (automated deployments)

After the first manual setup, use Cloud Build for all future deployments.

```bash
# Submit a build manually (replaces Steps 1–3 above in one command)
# This builds all 3 images, deploys the Cloud Run jobs, and syncs scripts + DAG
gcloud builds submit \
  --config=cloudbuild.yaml \
  --substitutions=\
_ENV=dv,\
_PROJECT_ID=dv-env,\
_REGION=us-central1,\
_REGISTRY=us-central1-docker.pkg.dev/dv-env/mb-repo,\
_SA_EMAIL=mb-pipeline-sa@dv-env.iam.gserviceaccount.com,\
_SOURCE_BUCKET=dv-mb-data-bucket,\
_PIPELINE_BUCKET=dv-mb-pipeline-bucket,\
_COMPOSER_BUCKET=<YOUR_COMPOSER_BUCKET_NAME>

# For PROD, repeat with prod values:
gcloud builds submit \
  --config=cloudbuild.yaml \
  --substitutions=\
_ENV=prod,\
_PROJECT_ID=prod-env,\
_REGION=us-central1,\
_REGISTRY=us-central1-docker.pkg.dev/prod-env/mb-repo,\
_SA_EMAIL=mb-pipeline-sa@prod-env.iam.gserviceaccount.com,\
_SOURCE_BUCKET=prod-mb-data-bucket,\
_PIPELINE_BUCKET=prod-mb-pipeline-bucket,\
_COMPOSER_BUCKET=<YOUR_PROD_COMPOSER_BUCKET_NAME>
```

> 💡 To set up **automatic triggers** (on git push to a branch), configure a Cloud Build trigger in the GCP Console → Cloud Build → Triggers, pointing to this repo and `cloudbuild.yaml`.

---

## 🔁 GCS Folder Structure (Expected After Pipeline Run)

```
gs://dv-mb-data-bucket/
├── landing/
│   ├── INDIA/          INDDST01_20260519.xlsx  ...
│   ├── AUSTRALIA/      AUSDST01_20260519.xlsx  ...
│   ├── CHINA/
│   ├── AMERICA/
│   ├── SOUTH KOREA/
│   └── SOUTH AFRICA/
├── transformed/
│   ├── samsung/        INDDST01_20260519.csv   ...
│   ├── apple/
│   ├── oppo/
│   ├── vivo/
│   └── oneplus/
├── archive_landing/
│   └── 2026-05-19/
│       ├── INDIA/
│       └── AUSTRALIA/  ...
└── archive_transformed/
    └── 2026-05-19/
        ├── samsung/
        └── apple/      ...

gs://dv-mb-pipeline-bucket/
├── scripts/
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
├── raw_data/                            ← Bronze layer (Parquet, partitioned by date)
│   ├── samsung/
│   │   └── date_reported=2026-05-19/   *.parquet
│   ├── apple/
│   │   └── date_reported=2026-05-19/   *.parquet
│   ├── oppo/
│   ├── vivo/
│   └── oneplus/
└── silver/
    └── mobile_brands/
        └── silver_brands_ingest_delta/  ← Silver Delta Lake table (all brands unified)
            ├── _delta_log/
            └── date_reported=2026-05-19/  *.parquet
```

> 📊 **BigQuery Gold Table**: `{project_id}.mobile_brands.gold_brand_daily_v1`
> Written directly by `gold.py` via the BigQuery Spark connector (no GCS parquet layer).

---

## 📋 Airflow DAG Execution Flow

```
generate_excel (Cloud Run: ingestion)
      ↓
transform_csv (Cloud Run: transform)
      ↓
create_cluster (Dataproc)
      ↓
bronze (PySpark: CSV → raw_data/<brand>/date_reported=<dt>/ Parquet)
      ↓
silver (PySpark: all brands → deduplicated Delta table via MERGE)
      ↓
gold (PySpark: Delta + BQ dims → nested schema → BigQuery gold_brand_daily_v1)
      ↓
archive (Cloud Run: move files to archive folders)
      ↓
delete_cluster (always runs — even if pipeline fails)
```

---

## 🔍 Troubleshooting

| Symptom | Where to look | Fix |
|---|---|---|
| Excel files not in `landing/` | Cloud Run logs for `mb-ingestion` | Check `BUCKET_NAME` env var; check SA storage permissions |
| CSVs missing from `transformed/` | Cloud Run logs for `mb-transform` | Verify file naming has `_YYYYMMDD`; check `RUN_DATE` passed |
| Bronze job reads 0 files | Dataproc job logs | Confirm `--source_bucket` matches where CSVs were written; check date format `_YYYYMMDD.csv` |
| Silver fails with `ClassNotFoundException` | Dataproc job logs | Delta Lake jar not loaded — confirm cluster image is `2.2-debian12` and `spark.jars.packages` is set |
| Silver MERGE fails (table not found) | Dataproc job logs | Check `mobile_brands` database exists in metastore or let first run create it |
| Silver drops too many rows | Dataproc job logs | Check `BUSINESS_KEY_COLS` match actual column names in the Bronze data |
| Gold BQ dims not found | Dataproc job logs | Confirm `dim_date`, `dim_market`, `product_v2`, `customer` tables exist in `{project}.mobile_brands` |
| Gold BigQuery write fails | Dataproc job logs | Confirm SA has `bigquery.dataEditor` + `bigquery.jobUser` roles; check `temporaryGcsBucket` accessible |
| Archive moves wrong day's files | Cloud Run logs for `mb-archive` | Confirm `RUN_DATE` is being passed from Airflow `{{ ds }}` |
| Cluster not deleted after failure | Airflow UI | `delete_cluster` has `trigger_rule=ALL_DONE` — check Airflow logs |
| Cloud Build fails at docker build | Cloud Build logs | Confirm folder names match: `ingestion/`, `transform/`, `archival/` |
| DAG not appearing in Airflow | Composer logs | Check DAG was uploaded to correct `gs://<COMPOSER_BUCKET>/dags/` path |

---

## ✅ Best Practices Implemented

| Practice | Where |
|---|---|
| Idempotent file naming (`_YYYYMMDD`) | ingestion, transform, archival |
| Append + partition overwrite (safe bronze re-runs) | bronze.py |
| Delta MERGE — idempotent incremental load | silver.py |
| BigQuery overwrite — full refresh gold | gold.py |
| Thread-safe parallel GCS operations | transform, archival |
| Ephemeral Dataproc cluster (cost saving) | DAG: create → run → delete |
| `trigger_rule=ALL_DONE` on cluster delete | Always cleans up even on failure |
| `max_active_runs=1` on DAG | No overlapping pipeline runs |
| `skip_if_exists` idempotency in transform | Safe to re-trigger without double-processing |
| `parse_known_args` in all Spark scripts | Scripts are resilient to extra DAG args |
| Non-zero exit on errors | Cloud Run jobs fail loudly so Airflow stops |
| Config/code separation | JSON configs per service — no hardcoded values |

---

## 🌐 Environment Summary

| Component | DV Job/Resource | PROD Job/Resource |
|---|---|---|
| Ingestion CR Job | `dv-mb-ingestion` | `prod-mb-ingestion` |
| Transform CR Job | `dv-mb-transform` | `prod-mb-transform` |
| Archive CR Job | `dv-mb-archive` | `prod-mb-archive` |
| Dataproc Cluster | `dv-mb-cluster-<date>` | `prod-mb-cluster-<date>` |
| Airflow DAG ID | `dv_mobile_brands_pipeline` | `prod_mobile_brands_pipeline` |
| Data Bucket | `dv-mb-data-bucket` | `prod-mb-data-bucket` |
| Pipeline Bucket | `dv-mb-pipeline-bucket` | `prod-mb-pipeline-bucket` |
| BigQuery Dataset | `dv-env:mobile_brands` | `prod-env:mobile_brands` |
| BigQuery Table | `gold_brand_daily_v1` | `gold_brand_daily_v1` |
| Dataproc Image | `2.2-debian12` (Spark 3.5) | `2.2-debian12` (Spark 3.5) |
| Delta Lake version | `delta-spark_2.12:3.2.0` | `delta-spark_2.12:3.2.0` |