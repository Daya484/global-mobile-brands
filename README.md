# Mobile Brands — End-to-End Data Pipeline

Production-grade GCP pipeline: **Cloud Run Jobs → Dataproc (PySpark) → BigQuery**, orchestrated by **Cloud Composer (Airflow)**.

---

## Repo Structure

```
├─ ingestion/              Cloud Run Job — reads Excel from GCS, writes CSVs to landing/
│  ├─ main.py
│  ├─ requirements.txt
│  └─ Dockerfile

├─ transform/              Cloud Run Job — cleans CSVs (multi-threaded), writes to transform/
│  ├─ main.py
│  ├─ transform.py
│  ├─ requirements.txt
│  └─ Dockerfile

├─ archival/               Cloud Run Job — moves landing/ + transform/ → archive/
│  ├─ main.py
│  ├─ requirements.txt
│  └─ Dockerfile

├─ dataproc_jobs/          PySpark jobs submitted to ephemeral Dataproc cluster
│  ├─ bronze.py            transform/ CSVs → bronze/ Parquet  (+DQ gate)
│  ├─ silver.py            bronze/ → silver/ (dedup, clean)
│  └─ gold.py              silver/ → gold/ Parquet + BigQuery write

├─ airflow/                Cloud Composer DAG
│  └─ dag_end_to_end_pipeline.py

├─ cloudbuild.yaml         CI/CD: build 3 Docker images, deploy Cloud Run Jobs, sync scripts
└─ .gcloudignore
```

---

## GCS Data Layout (Idempotent)

```
gs://<pipeline_bucket>/
  landing/dt=YYYY-MM-DD/run_id=<run_id>/<folder>/<brand>.csv   ← ingestion writes here
  transform/dt=YYYY-MM-DD/run_id=<run_id>/<folder>/<brand>.csv ← transform writes here
  bronze/brand=<brand>/dt=YYYY-MM-DD/*.parquet                  ← PySpark bronze
  silver/brand=<brand>/dt=YYYY-MM-DD/*.parquet                  ← PySpark silver
  gold/dt=YYYY-MM-DD/*.parquet                                   ← PySpark gold
  archive/landing/...                                            ← archived after success
  archive/transform/...
  scripts/bronze.py silver.py gold.py                           ← Dataproc job scripts
```

---

## Pipeline Flow (DAG)

```
ingestion (Cloud Run Job)
    ↓
transform (Cloud Run Job)
    ↓
create_dataproc_cluster  [ephemeral + preemptible workers]
    ↓
bronze → silver → gold   [PySpark — DQ gate inside bronze]
    ↓
delete_dataproc_cluster  [always runs, even on failure]
    ↓
archival (Cloud Run Job)
```

---

## Best Practices Applied

| # | Practice | Where |
|---|---------|-------|
| 1 | **Idempotent paths** `dt=` + `run_id=` | ingestion, transform |
| 2 | **Metadata columns** `ingest_ts`, `source_file`, `run_id` | bronze.py |
| 3 | **Data Quality gate** (row count, null check) | bronze.py |
| 4 | **Dedup / merge** by business keys | silver.py |
| 5 | **Ephemeral Dataproc** cluster (create→use→delete) | DAG |
| 6 | **Preemptible workers** (secondary_worker_config) | cluster config |
| 7 | **Cluster always deleted** (`TriggerRule.ALL_DONE`) | DAG |
| 8 | **Version tags** (`$SHORT_SHA`) on Docker images | cloudbuild.yaml |
| 9 | **Parallel archival** (ThreadPoolExecutor) | archival/main.py |
| 10 | **Dynamic partition overwrite** (safe re-runs) | all PySpark jobs |

---

## Environment Variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `SOURCE_BUCKET` | ingestion | Raw Excel source GCS bucket |
| `DEST_BUCKET` | ingestion, transform, archival | Pipeline GCS bucket |
| `RUN_ID` | all Cloud Run Jobs | Airflow run_id (injected at runtime) |
| `DT` | transform, archival | Date partition YYYY-MM-DD |
| `MB_ENV` | DAG (Airflow Variable) | `dv` or `pd` |
| `MB_PROJECT_ID` | DAG | GCP project ID |
| `MB_PIPELINE_BUCKET` | DAG | Pipeline bucket name |

---

## Deployment (Cloud Shell)

```bash
# 1. Set environment
export ENV=dv   # or pd

# 2. Build and deploy via Cloud Build
gcloud builds submit . \
  --config=cloudbuild.yaml \
  --substitutions="_ENV=${ENV},_PROJECT_ID=dv-env,_PIPELINE_BUCKET=dv-mb-pipeline-bucket" \
  --project=dv-env

# 3. Set Airflow Variables in Composer
gcloud composer environments run dv-mb-composer \
  --location=us-central1 --project=dv-env \
  variables -- set MB_ENV dv
```

---

## Airflow Variables to Set

```
MB_ENV             = dv
MB_PROJECT_ID      = dv-env
MB_REGION          = us-central1
MB_PIPELINE_BUCKET = dv-mb-pipeline-bucket
MB_SOURCE_BUCKET   = dv-mobile-brands
MB_BQ_DATASET      = mobile_brands
```
