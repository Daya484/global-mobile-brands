# 🛠️ Cloud Build CI/CD — Mobile Brands Pipeline

This guide explains how **Google Cloud Build** is used to automate building, testing, and deploying the entire data pipeline.

---

## 1. ❓ What is Cloud Build?
**Cloud Build** is Google Cloud's serverless continuous integration and continuous delivery (CI/CD) platform. It imports source code from repositories (such as GitHub), executes build steps based on your specifications, and deploys artifacts to GCP.

In this pipeline, every code change pushed to git automatically triggers a Cloud Build workflow that rebuilds Docker images and updates our jobs and DAGs.

---

## 2. 🎯 Why is it Used & What Does it Do?
Instead of manually running Docker commands, copying scripts to GCS, and configuring Cloud Run parameters every time the code changes, Cloud Build automates all of it:

1. **Compiles Docker Containers**: Runs `docker build` on the three Python scripts (`ingestion`, `transform`, `archival`) using high-speed cloud builders.
2. **Pushes to Artifact Registry**: Sends the output Docker images to Google Artifact Registry.
3. **Deploys Cloud Run Jobs**: Updates the Cloud Run Jobs with the new container image version.
4. **Synchronizes PySpark Scripts**: Copies PySpark files to the Dataproc scripts bucket.
5. **Synchronizes Airflow DAG**: Uploads the latest DAG file directly to the Composer bucket.

---

## 3. ⚙️ Trigger Setup & Configuration

Cloud Build configuration is defined in **`cloudbuild.yaml`**. 

### Step 3.1: Substitution Variables
The `cloudbuild.yaml` file uses substitution variables to remain environment-agnostic. When creating a Cloud Build Trigger, you define these variables to deploy to **DV** vs **PROD**:

| Variable | Description | Example (PD) |
|---|---|---|
| `_ENV` | Suffix for resource naming | `pd` |
| `_PROJECT_ID` | GCP Project ID | `pd-env-495516` |
| `_REGION` | Compute/Data Region | `us-central1` |
| `_REGISTRY` | Docker repository URL | `us-central1-docker.pkg.dev/pd-env-495516/mb-repo` |
| `_SA_EMAIL` | Service account for job runs | `mb-pipeline-sa@pd-env-495516.iam.gserviceaccount.com` |
| `_SOURCE_BUCKET` | Landed files GCS bucket | `mb-data-pd-495516` |
| `_PIPELINE_BUCKET` | Processing files GCS bucket | `mb-pipeline-pd-495516` |
| `_COMPOSER_BUCKET` | Composer DAGs bucket name | `us-central1-pd-environment--bdf24ecd-bucket` |

---

## 🛠️ How to Deploy & Run

### Step 3.2: Create a Cloud Build Trigger in GCP Console
To set up automated deployments on git push:
1. Connect your GitHub repository to Google Cloud Build in the GCP Console (**Cloud Build** > **Repositories**).
2. Go to **Triggers** and click **Create Trigger**.
3. Configure settings:
   * **Name**: `mb-pipeline-prod-deploy`
   * **Event**: Push to a branch
   * **Branch**: `^pd-env$` (or `^coding-branch-daya$` for DV)
   * **Configuration**: Cloud Build configuration file (`cloudbuild.yaml`)
   * **Substitution Variables**: Add all the keys and values from the table above.

### Step 3.3: Submit a Manual Build (For Testing)
To build and deploy the entire pipeline instantly without pushing code to GitHub, run the `gcloud builds submit` command in Cloud Shell:

```bash
gcloud builds submit \
  --config=cloudbuild.yaml \
  --substitutions=\
SHORT_SHA=$(git rev-parse --short HEAD),\
_ENV=pd,\
_PROJECT_ID=pd-env-495516,\
_REGION=us-central1,\
_REGISTRY=us-central1-docker.pkg.dev/pd-env-495516/mb-repo,\
_SA_EMAIL=mb-pipeline-sa@pd-env-495516.iam.gserviceaccount.com,\
_SOURCE_BUCKET=mb-data-pd-495516,\
_PIPELINE_BUCKET=mb-pipeline-pd-495516,\
_COMPOSER_BUCKET=us-central1-pd-environment--bdf24ecd-bucket
```
*(Once this completes, your Docker images, Cloud Run Jobs, PySpark scripts, and DAG will all be updated to the latest version of your code).*
