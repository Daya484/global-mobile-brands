Perfect ✅ — here is your final step‑by‑step README.md specifically for deploying Cloud Build (DV + PROD)
👉 This is focused only on CI/CD deployment using Cloud Build
👉 You can copy this fully into your repo

✅ ✅ ✅ FINAL README.md — Cloud Build Deployment (DV + PROD)
🚀 Cloud Build Deployment Guide (DV + PROD)

This document explains how to deploy the entire pipeline using **Cloud Build CI/CD**.
✅ Builds Docker images  
✅ Deploys Cloud Run Jobs 
✅ Syncs Dataproc scripts  
✅ Syncs Airflow DAG  -

--# 
✅ 1. OverviewPipeline deployment happens automatically using **Cloud Build**:``
Git Push
↓
Cloud Build Trigger
↓
Build Docker Images
↓
Deploy Cloud Run Jobs
↓
Upload Dataproc Scripts
↓
Upload Airflow DAG

---

# ✅ 2. Repository Structure


repo/
├── cloudbuild.yaml
├── 01-ingestion/
├── 02-transform/
├── 03-archival/
├── 04-dataproc_jobs/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
├── 05-airflow/
│   └── dag_mb_pipeline.py

---

# ✅ 3. Prerequisites

✅ Install Google Cloud SDK  
✅ Access to DV and PROD projects  
✅ Enable APIs:

```bash
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  dataproc.googleapis.com \
  composer.googleapis.com \
  storage.googleapis.com


✅ 4. Create Artifact Registry (one-time)
✅ DV
Shellgcloud artifacts repositories create mb-repo \  --repository-format=docker \  --location=us-central1 \  --project=dv-env
✅ PROD
Shellgcloud artifacts repositories create mb-repo \  --repository-format=docker \  --location=us-central1 \  --project=prod-env

✅ 5. Create Service Account
Shellgcloud iam service-accounts create mb-pipeline-sa``

✅ Assign Roles
Shellgcloud projects add-iam-policy-binding PROJECT_ID \  --member="serviceAccount:mb-pipeline-sa@PROJECT_ID.iam.gserviceaccount.com" \  --role="roles/run.admin"gcloud projects add-iam-policy-binding PROJECT_ID \  --member="serviceAccount:mb-pipeline-sa@PROJECT_ID.iam.gserviceaccount.com" \  --role="roles/storage.admin"gcloud projects add-iam-policy-binding PROJECT_ID \  --member="serviceAccount:mb-pipeline-sa@PROJECT_ID.iam.gserviceaccount.com" \  --role="roles/dataproc.editor"

✅ 6. Cloud Build Configuration
Your pipeline uses cloudbuild.yaml.
👉 This file:
✔ Builds ingestion image
✔ Builds transform image
✔ Builds archive image
✔ Deploys Cloud Run jobs
✔ Uploads scripts to GCS
✔ Uploads DAG to Composer

✅ 7. Create Cloud Build Trigger (CI/CD)
✅ Connect to GitHub
Shellgcloud builds triggers create github \  --repo-name=YOUR_REPO_NAME \  --repo-owner=YOUR_GITHUB_USERNAME \  --branch-pattern="^main$" \  --build-config=cloudbuild.yaml

✅ What this does
Every time you push code:
✅ Images rebuilt
✅ Cloud Run jobs updated
✅ DAG updated
✅ Dataproc scripts updated

✅ 8. First Manual Run
Run once manually:
Shellgcloud builds submit --project=dv-env``

✅ 9. Cloud Run Jobs (Auto Deployment)
After build completes → verify:
👉 GCP Console → Cloud Run → Jobs
✅ DV Jobs
dv-mb-ingestion
dv-mb-transform
dv-mb-archive


✅ PROD Jobs
prod-mb-ingestion
prod-mb-transform
prod-mb-archive


✅ 10. Dataproc Scripts
Uploaded automatically to:
gs://<PIPELINE_BUCKET>/scripts/
  bronze.py
  silver.py
  gold.py


✅ 11. Airflow DAG Deployment
Uploaded automatically to:
gs://<COMPOSER_BUCKET>/dags/


✅ 12. Environment Configuration (IMPORTANT)

✅ DEV (DV)
YAML_ENV: dv_PROJECT_ID: dv-env_PIPELINE_BUCKET: dv-mb-pipeline-bucket_REGISTRY: us-central1-docker.pkg.dev/dv-env/mb-repo``

✅ PROD
YAML_ENV: prod_PROJECT_ID: prod-env_PIPELINE_BUCKET: prod-mb-pipeline-bucket_REGISTRY: us-central1-docker.pkg.dev/prod-env/mb-repo

✅ 13. How Deployment Works

✅ When you PUSH code
git push origin main

👉 Automatically:
✅ Cloud Build runs
✅ New Docker images built
✅ Cloud Run jobs updated
✅ DAG updated
✅ Scripts updated

✅ 14. What happens DAILY
👉 Airflow DAG runs:
Generator → Transform → Dataproc → Archive

❌ No image rebuild
❌ No redeploy
✅ Only execution

✅ 15. Verify Deployment

✅ Check Cloud Build
GCP → Cloud Build → History


✅ Check Cloud Run Jobs
gcloud run jobs list


✅ Check GCS
landing/
transformed/
archive/


✅ Check Airflow
Trigger DAG from UI

✅ 16. Troubleshooting

Issue                 Solution
Build fails           Check Cloud Build logs
Jobs not updated      Ensure trigger is configured
DAG not updating      Validate composer bucket
Cloud Run error       Check Cloud Run logs
Wrong env             Verify substitutions

✅ 17. DV vs PROD Deployment Strategy

✅ DV Deployment
Project: dv-env
Trigger: dv branch or main
Jobs: dv-mb-*


✅ PROD Deployment
Project: prod-env
Trigger: release branch
Jobs: prod-mb-*


✅ ✅ ✅ FINAL SUMMARY
✅ Cloud Build = Deployment (only when code changes)
✅ Airflow = Execution (runs daily pipeline)

✅ ✅ ✅ RESULT
After setup:
✔ Fully automated CI/CD pipeline
✔ Multi-env support (DV + PROD)
✔ No manual deployment needed
✔ Git push → everything updated