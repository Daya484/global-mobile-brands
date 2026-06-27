# ☁️ Cloud Run Jobs — Mobile Brands Pipeline

This guide explains the role, architecture, and deployment of **Google Cloud Run Jobs** within the Mobile Brands data engineering pipeline.

---

## 1. ❓ What is Cloud Run?
**Cloud Run** is a Google Cloud serverless container platform. It allows you to run containerized applications without managing any underlying server infrastructure. 

In this pipeline, we use **Cloud Run Jobs** (rather than Cloud Run Services). While *Services* are designed to listen for web requests (like a website), *Jobs* are designed to run to completion (like a script or batch process) and shut down immediately when done.

---

## 2. 🎯 Why is it Used & What Does it Do?
We use Cloud Run Jobs to perform the non-Spark, lightweight Python tasks in our pipeline:
1. **`ingestion` (Extract)**: 
   * **What it does**: Reads raw Excel spreadsheet files from an input landing area, extracts the data, and writes unified CSV files to GCS.
   * **Why Cloud Run**: Executing Python Pandas Excel extraction in a serverless container is extremely cost-effective and doesn't require maintaining a running server.
2. **`transform` (Transform - Stage 1)**: 
   * **What it does**: Cleans, standardizes, and formats raw CSVs using Python multi-threading, preparing them for Spark processing.
   * **Why Cloud Run**: Scales resources dynamically based on file size.
3. **`archival` (Archive)**: 
   * **What it does**: Collects all processed landing and staging files and moves them to archival GCS paths using parallel threads.
   * **Why Cloud Run**: Fast, clean file moves without wasting Spark compute resources.

---

## 3. 🛠️ How to Build and Deploy

Each job has its own directory containing:
* `main.py` (application logic)
* `requirements.txt` (dependencies)
* `Dockerfile` (defines the container environment)

### Step 3.1: Build Docker Images using Cloud Build
Instead of building images locally (which can fail due to network constraints in Cloud Shell), we submit builds to **Cloud Build** which compiles them in the cloud and pushes them to **Artifact Registry**:

```bash
# Set your target Artifact Registry path (replace project ID and region)
export REGISTRY="us-central1-docker.pkg.dev/pd-env-495516/mb-repo"

# Build Ingestion
gcloud builds submit ./ingestion --tag ${REGISTRY}/mb-ingestion:latest

# Build Transform
gcloud builds submit ./transform --tag ${REGISTRY}/mb-transform:latest

# Build Archival
gcloud builds submit ./archival --tag ${REGISTRY}/mb-archival:latest
```

### Step 3.2: Create or Update Cloud Run Jobs
Deploy the container images as Cloud Run Jobs in your project:

```bash
export REGION="us-central1"
export SA_EMAIL="mb-pipeline-sa@pd-env-495516.iam.gserviceaccount.com"
export DATA_BUCKET="mb-data-pd-495516"

# 1. Create Ingestion Job
gcloud run jobs create mb-ingestion-job \
  --image ${REGISTRY}/mb-ingestion:latest \
  --region $REGION \
  --service-account $SA_EMAIL \
  --set-env-vars BUCKET_NAME=$DATA_BUCKET,ENV=pd

# 2. Create Transform Job
gcloud run jobs create mb-transform-job \
  --image ${REGISTRY}/mb-transform:latest \
  --region $REGION \
  --service-account $SA_EMAIL \
  --set-env-vars BUCKET_NAME=$DATA_BUCKET,ENV=pd

# 3. Create Archival Job
gcloud run jobs create mb-archival-job \
  --image ${REGISTRY}/mb-archival:latest \
  --region $REGION \
  --service-account $SA_EMAIL \
  --set-env-vars BUCKET_NAME=$DATA_BUCKET,ENV=pd
```

---

## ⚡ Manual Execution (For Testing)
To trigger and test a Cloud Run Job manually from the command line:

```bash
# Run Ingestion
gcloud run jobs execute mb-ingestion-job --region=us-central1 --wait

# Run Transform
gcloud run jobs execute mb-transform-job --region=us-central1 --wait

# Run Archival
gcloud run jobs execute mb-archival-job --region=us-central1 --wait
```
