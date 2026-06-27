# ⚡ Dataproc Serverless — Mobile Brands Pipeline

This guide explains the PySpark data processing architecture running on **Google Cloud Dataproc Serverless**.

---

## 1. ❓ What is Dataproc Serverless?
**Google Cloud Dataproc** is a managed Apache Spark and Hadoop service. **Dataproc Serverless** allows you to submit Spark batch jobs directly without creating, managing, or scaling a running Spark cluster. Google dynamically provisions ephemeral compute instances for the job and destroys them immediately when the job finishes.

This model is extremely cost-effective as we pay only for the exact seconds our processing scripts run.

---

## 2. 🎯 Why is it Used & What Does it Do?
We use Dataproc Serverless to process large amounts of mobile brands transaction data through three processing layers (Bronze → Silver → Gold):

1. **Bronze (`bronze.py`)**:
   * **What it does**: Reads CSVs from `stage/transformed` GCS bucket paths, adds metadata columns (ingestion timestamp, source file name, run ID), enforces a **Data Quality (DQ) Gate**, and writes the data to GCS partitioned by date as Parquet.
2. **Silver (`silver.py`)**:
   * **What it does**: Reads the Bronze Parquet files, cleans invalid characters, standardizes schema types, de-duplicates records based on business keys, and merges them into a unified dataset.
3. **Gold (`gold.py`)**:
   * **What it does**: Reads the Silver dataset, joins it with 4 dimension tables from BigQuery (`dim_date`, `dim_market`, `product_v2`, `customer`) using a **Salted Join** to avoid data skew, structures the data into highly query-optimized nested records (STRUCTs), and writes the output directly into a BigQuery table.

---

## 3. 🔑 IAM Permissions & Configuration

Because Dataproc Serverless runs on ephemeral VM instances and communicates with GCS and BigQuery, your pipeline service account (**`mb-pipeline-sa@pd-env-495516.iam.gserviceaccount.com`**) requires specific IAM roles:

* **`roles/dataproc.editor`**: Required by Airflow to submit Spark jobs.
* **`roles/dataproc.worker`**: Required by the Spark VM instance runtime to initialize and log data.
* **`roles/iam.serviceAccountUser`**: Required for the Composer workers to submit jobs under the identity of the service account.
* **`roles/bigquery.readSessionUser`**: Required for the Spark BigQuery connector to establish parallel read sessions when loading the BigQuery dimension tables.
* **`roles/storage.objectAdmin`**: Required to read/write Parquet files.
* **`roles/bigquery.dataEditor` & `roles/bigquery.jobUser`**: Required to write the Gold layer output into BigQuery.

---

## 🛠️ How to Deploy & Run

### Step 3.1: Upload PySpark scripts to GCS
The PySpark scripts must be uploaded to the pipeline bucket so that Dataproc Serverless can read and execute them:

```bash
# Upload scripts to GCS
gcloud storage cp dataproc_jobs/ gs://mb-pipeline-pd-495516/scripts/ --recursive
```

### Step 3.2: Configure Private Google Access (Subnet Prerequisite)
Dataproc Serverless requires Private Google Access to be enabled on the subnet where it runs. Run this in Cloud Shell:

```bash
gcloud compute networks subnets update default \
  --region=us-central1 \
  --enable-private-ip-google-access
```

### Step 3.3: Submit a Dataproc Batch manually (For Testing)
To test a PySpark job manually from the command line, use the `gcloud dataproc batches submit` command:

```bash
gcloud dataproc batches submit pyspark gs://mb-pipeline-pd-495516/scripts/bronze.py \
  --region=us-central1 \
  --batch=bronze-manual-run \
  --service-account=mb-pipeline-sa@pd-env-495516.iam.gserviceaccount.com \
  -- --pipeline_bucket=mb-pipeline-pd-495516 --project_id=pd-env-495516 --env=pd
```
*(In production, this submission is handled automatically by Airflow's `DataprocCreateBatchOperator`).*
