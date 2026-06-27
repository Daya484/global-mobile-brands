# 🌪️ Airflow & Cloud Composer — Mobile Brands Pipeline

This guide explains the orchestration, variables, and notification configuration for **Apache Airflow** running inside **Google Cloud Composer**.

---

## 1. ❓ What is Cloud Composer / Airflow?
**Apache Airflow** is an open-source workflow management platform. **Cloud Composer** is Google Cloud's fully-managed service for running Airflow. 

We write our pipeline DAG (Directed Acyclic Graph) in Python, defining which tasks run, their order of execution, what parameters they use, and how they handle failures.

---

## 2. 🎯 Why is it Used & What Does it Do?
Composer/Airflow acts as the **brain** of the pipeline:
1. **Dependency Orchestration**: It ensures that `transform` runs only after `ingestion` completes, and Spark `silver` runs only after `bronze` completes.
2. **Dynamic Configurations**: It passes runtime variables (like `run_date` and bucket paths) down to the Cloud Run jobs and Dataproc Spark scripts.
3. **Failure Recovery & Alerts**: If any job fails, it catches the error, triggers failure handlers, and emails alerts.
4. **Quota Release Waits**: Uses Python operators to inject short wait periods between heavy workloads, allowing GCP to release compute resources.

---

## 3. 🛠️ How to Deploy & Configure

### Step 3.1: Find your Composer GCS Bucket
Airflow reads DAG files directly from a designated Cloud Storage bucket. To retrieve your environment's bucket name:

```bash
# Replace environment name and location if needed
gcloud composer environments describe pd-environment-dag \
  --location=us-central1 \
  --format="value(config.dagGcsPrefix)"
```
Copy the bucket name from the output (e.g., `us-central1-pd-environment--bdf24ecd-bucket`).

### Step 3.2: Upload the DAG File
Copy your DAG script to the `/dags` folder of your Composer GCS bucket:

```bash
# Upload to Composer
gcloud storage cp airflow/dag_end_to_end_pipeline.py \
  gs://us-central1-pd-environment--bdf24ecd-bucket/dags/dag_end_to_end_pipeline.py
```

### Step 3.3: Set Airflow Variables
Airflow Variables keep the pipeline code environment-agnostic. Set the following variables in Cloud Shell:

```bash
# Target environment (dv or pd)
gcloud composer environments run pd-environment-dag \
  --location=us-central1 variables -- set MB_ENV pd

# Target GCP Project ID
gcloud composer environments run pd-environment-dag \
  --location=us-central1 variables -- set MB_PROJECT_ID pd-env-495516

# Region for jobs
gcloud composer environments run pd-environment-dag \
  --location=us-central1 variables -- set MB_REGION us-central1

# GCS Source Bucket
gcloud composer environments run pd-environment-dag \
  --location=us-central1 variables -- set MB_SOURCE_BUCKET mb-data-pd-495516

# GCS Pipeline Bucket (Spark jars/staging)
gcloud composer environments run pd-environment-dag \
  --location=us-central1 variables -- set MB_PIPELINE_BUCKET mb-pipeline-pd-495516

# BigQuery Dataset Name
gcloud composer environments run pd-environment-dag \
  --location=us-central1 variables -- set MB_BQ_DATASET mobile_brands
```

---

## 📧 4. Set Up Email Notifications (SMTP)
To get actual emails when a DAG fails or completes successfully, configure Gmail SMTP overrides:

### Step 4.1: Create Gmail App Password
1. Go to your [Google Account Security Settings](https://myaccount.google.com/security) and ensure **2-Step Verification** is enabled.
2. Search for **App passwords**.
3. Create a new app password for "Airflow" and copy the 16-character code (`xxxx xxxx xxxx xxxx`).

### Step 4.2: Register the Connection & Apply Configs in Cloud Shell
Run these two commands in your terminal to save your SMTP credentials and configure Airflow parameters:

```bash
# 1. Create the Airflow Connection (replace ykqqzaokttfyqfad with your App Password without spaces)
gcloud composer environments run pd-environment-dag \
  --location=us-central1 \
  connections add -- smtp_default \
  --conn-type=email \
  --conn-host=smtp.gmail.com \
  --conn-login=dayasagarreddy2943@gmail.com \
  --conn-password=ykqqzaokttfyqfad \
  --conn-port=587

# 2. Add SMTP configuration overrides
gcloud composer environments update pd-environment-dag \
  --location=us-central1 \
  --update-airflow-configs=email-email_backend=airflow.utils.email.send_email_smtp,smtp-smtp_host=smtp.gmail.com,smtp-smtp_port=587,smtp-smtp_ssl=False,smtp-smtp_starttls=True,smtp-smtp_user=dayasagarreddy2943@gmail.com,smtp-smtp_mail_from=dayasagarreddy2943@gmail.com
```
*(Wait 5 minutes for the Composer environment to apply update before running/retrying DAG).*
