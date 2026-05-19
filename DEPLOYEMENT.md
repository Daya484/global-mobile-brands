You want a complete README.md that explains deployment for:
✅ DV + PROD environments
✅ 3 Cloud Run jobs (Generator, Transformer, Archive)
✅ Dataproc (Bronze, Silver, Gold)
✅ Airflow DAG (Composer)

✅ ✅ ✅ FINAL README.md (Production Ready)
👉 You can copy this directly into your repo

# 📊 Mobile Brands End-to-End Data Pipeline (GCP)

## ✅ Overview

This project implements a full **data pipeline on GCP**:

Cloud Run (Generator)
↓
Cloud Run (Transform)
↓
Dataproc (Bronze → Silver → Gold)
↓
Cloud Run (Archive)
↓
BigQuery (Final Output)

---

# ✅ Environments Supported

| Environment | Project ID | Bucket |
|-------------|-----------|--------|
| DEV (DV)    | dv-env    | mobile_brands |
| PROD        | prod-env  | mobile_brands |

👉 Buckets have same name but exist in different projects.

---

# ✅ GCS Bucket Structure

mobile_brands/
landing/
AUSTRALIA/
INDIA/
transformed/
apple/
samsung/
archive_landing/
YYYY-MM-DD/
AUSTRALIA/
archive_transformed/
YYYY-MM-DD/
apple/

---

# ✅ Step 1: Deploy Cloud Run Jobs

## ✅ 1. Generator Job (Excel creation)

### Build Image

```bash
gcloud builds submit --tag gcr.io/<PROJECT_ID>/generator

Create Job
Shellgcloud run jobs create <env>-generator \  --image gcr.io/<PROJECT_ID>/generator \  --region us-central1

✅ 2. Transform Job (Excel → CSV)
Shellgcloud builds submit --tag gcr.io/<PROJECT_ID>/transformgcloud run jobs create <env>-transform \  --image gcr.io/<PROJECT_ID>/transform \  --region us-central1

✅ 3. Archive Job
Shellgcloud builds submit --tag gcr.io/<PROJECT_ID>/archivegcloud run jobs create <env>-archive \  --image gcr.io/<PROJECT_ID>/archive \  --region us-central1

✅ Naming Convention 

ENV     Jobs  
DV      dv-generator, dv-transform, dv-archive 
PROD    prod-generator, prod-transform, prod-archive

✅ Step 2: Upload Dataproc Scripts
Upload PySpark jobs to GCS:
Shellgs://dv-mb-pipeline-bucket/scripts/    bronze.py    silver.py    gold.py

✅ Step 3: Dataproc Cluster Setup (Airflow handles this)
Cluster will be created dynamically using:

n1-standard-4
2 workers
preemptible nodes ✅ (cost saving)
auto delete ✅


✅ Step 4: Deploy Airflow (Cloud Composer)
✅ Create Composer Environment
Shellgcloud composer environments create mb-composer \  --location us-central1 \  --zone us-central1-a

✅ Upload DAG
Shellgsutil cp dag_mb_pipeline.py \gs://<COMPOSER_BUCKET>/dags/

✅ Step 5: Airflow Variables Setup
Set variables in Airflow UI:
✅ DEV
MB_ENV = dv
MB_PROJECT_ID = dv-env
MB_BUCKET = mobile_brands

✅ PROD
MB_ENV = prod
MB_PROJECT_ID = prod-env
MB_BUCKET = mobile_brands


✅ Step 6: Airflow DAG Execution Flow
generate_excel (Cloud Run)
        ↓
transform_csv (Cloud Run)
        ↓
create_dataproc_cluster
        ↓
bronze → silver → gold
        ↓
archive (Cloud Run)
        ↓
delete_cluster (always)


✅ Step 7: Trigger DAG
Manual Run
Airflow UI → Trigger DAG

Scheduled Run
0 1 * * *

(Runs daily)

✅ Step 8: Airflow Cloud Run ENV
All jobs receive:
RUN_DATE={{ ds }}
BUCKET_NAME={{ var.value.MB_BUCKET }}
PROJECT_ID={{ var.value.MB_PROJECT_ID }}


✅ Step 9: DV vs PROD Execution
DV
Project → dv-env
Job → dv-generator
Bucket → mobile_brands (DV)


PROD
Project → prod-env
Job → prod-generator
Bucket → mobile_brands (PROD)


✅ Step 10: Final Outputs
✅ Landing
landing/AUSTRALIA/AUSDST01_20260509.xlsx

✅ Transformed
transformed/apple/AUSDST01_20260509.csv

✅ Archived
archive_landing/2026-05-09/AUSTRALIA/...
archive_transformed/2026-05-09/apple/...


✅ Best Practices Used ✅
✅ Idempotent file naming
✅ Partition-based processing (date-driven)
✅ Environment-based job separation
✅ Ephemeral Dataproc cluster (cost-saving)
✅ Parallel processing
✅ Safe archive (copy + delete)
✅ Airflow retry + failure handling

✅ Troubleshooting

Issue                         Fix
Files not generated           Check generator Cloud Run logs
Transform missing             Check file naming (_YYYYMMDD)
Archive not moving            Ensure RUN_DATE passed
Dataproc failure              Check Spark logs
Bucket mismatch               Verify Airflow variables

✅ Future Enhancements
✅ Data quality validation in DAG
✅ Alerts (Email / Teams)
✅ Metadata tracking
✅ Audit tables in BigQuery
✅ CI/CD using Cloud Build