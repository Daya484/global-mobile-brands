# 🚀 Cloud Build CI/CD Deployment Guide — Mobile Brands Pipeline

> **Who is this for?** Engineers setting up or maintaining automated CI/CD for this pipeline.
> Every command is annotated. Follow steps in order — Steps 0–2 are one-time setup only.

---

## 📌 What Cloud Build Does in This Project

Every time code is pushed to the configured branch, Cloud Build automatically:

```
git push
    ↓
Cloud Build Trigger fires
    ↓
Step 1 → Build Docker image: mb-ingestion
Step 2 → Push mb-ingestion to Artifact Registry
Step 3 → Build Docker image: mb-transform
Step 4 → Push mb-transform to Artifact Registry
Step 5 → Build Docker image: mb-archival
Step 6 → Push mb-archival to Artifact Registry
Step 7 → Deploy Cloud Run Job: ${ENV}-mb-ingestion
Step 8 → Deploy Cloud Run Job: ${ENV}-mb-transform
Step 9 → Deploy Cloud Run Job: ${ENV}-mb-archive
Step 10 → Upload bronze.py, silver.py, gold.py → gs://<PIPELINE_BUCKET>/scripts/
Step 11 → Upload DAG → gs://<COMPOSER_BUCKET>/dags/
```

> ✅ Cloud Build = **Deployment** (runs when code changes)
> ✅ Airflow = **Execution** (runs the pipeline daily at 01:00 UTC)

---

## ⚙️ Substitution Variables in `cloudbuild.yaml`

These are the variables Cloud Build injects at build time.
They can be overridden per trigger (DV vs PROD).

| Variable | DV Default | PROD Value | Purpose |
|---|---|---|---|
| `_ENV` | `dv` | `prod` | Prefix for all job/resource names |
| `_PROJECT_ID` | `dv-env` | `prod-env` | GCP project |
| `_REGION` | `us-central1` | `us-central1` | Region for all resources |
| `_REGISTRY` | `us-central1-docker.pkg.dev/dv-env/mb-repo` | `...prod-env/mb-repo` | Artifact Registry path |
| `_SA_EMAIL` | `mb-pipeline-sa@dv-env.iam.gserviceaccount.com` | `...@prod-env...` | Service account for jobs |
| `_SOURCE_BUCKET` | `dv-mb-data-bucket` | `prod-mb-data-bucket` | Holds `landing/` & `transformed/` |
| `_PIPELINE_BUCKET` | `dv-mb-pipeline-bucket` | `prod-mb-pipeline-bucket` | Holds `scripts/`, `bronze/`, `silver/`, `gold/` |
| `_COMPOSER_BUCKET` | `us-central1-dv-mb-composer-XXXX` | `us-central1-prod-mb-composer-XXXX` | Composer DAGs bucket |

> 💡 Run all Cloud Shell commands from the **repo root** (`global-mobile-brands/`).

---

## 🔧 STEP 0 — One-Time: Grant Cloud Build Service Account Permissions

Cloud Build uses its own service account to deploy resources.
You must grant it the right roles once per project.

```bash
# ── Set your project ──────────────────────────────────────────────────────────
export PROJECT_ID="dv-env"        # change to prod-env for PROD
export PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")

# Cloud Build's default service account
export CB_SA="${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com"

echo "Cloud Build SA: $CB_SA"

# ── Grant roles so Cloud Build can build images, deploy CR jobs, push to GCS ──
# Allows Cloud Build to deploy Cloud Run Jobs
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CB_SA" \
  --role="roles/run.admin"

# Allows Cloud Build to act as the pipeline service account (for --service-account flag)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CB_SA" \
  --role="roles/iam.serviceAccountUser"

# Allows Cloud Build to push images to Artifact Registry
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CB_SA" \
  --role="roles/artifactregistry.writer"

# Allows Cloud Build to upload scripts and DAG to GCS
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CB_SA" \
  --role="roles/storage.objectAdmin"

# Allows Cloud Build to write logs
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CB_SA" \
  --role="roles/logging.logWriter"

# Verify roles were applied
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:$CB_SA" \
  --format="table(bindings.role)"
```

---

## 🔧 STEP 1 — One-Time: Create Artifact Registry Repository

Docker images are stored in Artifact Registry (one repo per project).

```bash
# ── DV ────────────────────────────────────────────────────────────────────────
# Create the Docker image repository for DV environment
gcloud artifacts repositories create mb-repo \
  --repository-format=docker \
  --location=us-central1 \
  --project=dv-env \
  --description="Mobile Brands pipeline Docker images"

# Verify the repo was created
gcloud artifacts repositories list \
  --location=us-central1 \
  --project=dv-env

# ── PROD ─────────────────────────────────────────────────────────────────────
# Repeat for PROD project
gcloud artifacts repositories create mb-repo \
  --repository-format=docker \
  --location=us-central1 \
  --project=prod-env \
  --description="Mobile Brands pipeline Docker images"
```

---

## 🔧 STEP 2 — One-Time: Find Your Composer Bucket Name

The Composer bucket is auto-generated. You need it for the trigger substitution.

```bash
# ── Get DV Composer bucket name ───────────────────────────────────────────────
# After your Composer environment is created, get the DAGs bucket name
gcloud composer environments describe mb-composer \
  --location=us-central1 \
  --project=dv-env \
  --format="value(config.dagGcsPrefix)"
# Example output: gs://us-central1-mb-composer-abc123-bucket/dags
# Your bucket name = us-central1-mb-composer-abc123-bucket  ← copy this

# ── Get PROD Composer bucket name ─────────────────────────────────────────────
gcloud composer environments describe mb-composer \
  --location=us-central1 \
  --project=prod-env \
  --format="value(config.dagGcsPrefix)"
```

> 📝 **Save these bucket names** — you need them in STEP 3 and STEP 4.

---

## 🔗 STEP 3 — Connect GitHub Repository to Cloud Build

Do this once to link your GitHub repo so Cloud Build can listen for pushes.

```bash
# ── Option A: Connect via gcloud (GitHub App must be installed first) ─────────
# Go to GCP Console → Cloud Build → Repositories → Connect Repository
# Then come back here to create triggers.

# ── Option B: Check existing connections ─────────────────────────────────────
gcloud builds connections list --region=us-central1 --project=dv-env
```

> 💡 **GitHub App**: Visit https://console.cloud.google.com/cloud-build/repositories
> and click **"Connect Repository"** → follow the OAuth flow to install the Google Cloud Build GitHub App on your repo.

---

## ⚡ STEP 4 — Create Cloud Build Triggers

Create one trigger per environment. Each trigger watches a specific branch and overrides substitution variables for that environment.

### 4a. DV Trigger — fires on push to `coding-branch-daya`

```bash
# Create the DV trigger
# This runs cloudbuild.yaml with DV substitutions every time you push to coding-branch-daya
gcloud builds triggers create github \
  --name="mb-pipeline-dv-deploy" \
  --repo-name="global-mobile-brands" \
  --repo-owner="Daya484" \
  --branch-pattern="^coding-branch-daya$" \
  --build-config="cloudbuild.yaml" \
  --project=dv-env \
  --region=us-central1 \
  --substitutions=\
_ENV=dv,\
_PROJECT_ID=dv-env,\
_REGION=us-central1,\
_REGISTRY=us-central1-docker.pkg.dev/dv-env/mb-repo,\
_SA_EMAIL=mb-pipeline-sa@dv-env.iam.gserviceaccount.com,\
_SOURCE_BUCKET=dv-mb-data-bucket,\
_PIPELINE_BUCKET=dv-mb-pipeline-bucket,\
_COMPOSER_BUCKET=us-central1-dv-mb-composer-XXXX

# Verify the trigger was created
gcloud builds triggers list --project=dv-env --region=us-central1
```

### 4b. PROD Trigger — fires on push to `pd-env`

```bash
# Create the PROD trigger
# Uses the pd-env branch and PROD substitution values
gcloud builds triggers create github \
  --name="mb-pipeline-prod-deploy" \
  --repo-name="global-mobile-brands" \
  --repo-owner="Daya484" \
  --branch-pattern="^pd-env$" \
  --build-config="cloudbuild.yaml" \
  --project=prod-env \
  --region=us-central1 \
  --substitutions=\
_ENV=prod,\
_PROJECT_ID=prod-env,\
_REGION=us-central1,\
_REGISTRY=us-central1-docker.pkg.dev/prod-env/mb-repo,\
_SA_EMAIL=mb-pipeline-sa@prod-env.iam.gserviceaccount.com,\
_SOURCE_BUCKET=prod-mb-data-bucket,\
_PIPELINE_BUCKET=prod-mb-pipeline-bucket,\
_COMPOSER_BUCKET=us-central1-prod-mb-composer-XXXX

# Verify
gcloud builds triggers list --project=prod-env --region=us-central1
```

> ⚠️ Replace `XXXX` in `_COMPOSER_BUCKET` with the real bucket name from STEP 2.

---

## 🧪 STEP 5 — First Manual Build (Test Before Relying on Auto-Trigger)

Run the build manually first to confirm everything works before enabling the auto-trigger.

### 5a. Manual DV build

```bash
# Manually submit the build for DV
# This runs exactly what the trigger would run on a git push
gcloud builds submit \
  --config=cloudbuild.yaml \
  --project=dv-env \
  --region=us-central1 \
  --substitutions=\
_ENV=dv,\
_PROJECT_ID=dv-env,\
_REGION=us-central1,\
_REGISTRY=us-central1-docker.pkg.dev/dv-env/mb-repo,\
_SA_EMAIL=mb-pipeline-sa@dv-env.iam.gserviceaccount.com,\
_SOURCE_BUCKET=dv-mb-data-bucket,\
_PIPELINE_BUCKET=dv-mb-pipeline-bucket,\
_COMPOSER_BUCKET=us-central1-dv-mb-composer-XXXX
```

### 5b. Manual PROD build

```bash
# Manually submit the build for PROD (run only after DV is confirmed working)
gcloud builds submit \
  --config=cloudbuild.yaml \
  --project=prod-env \
  --region=us-central1 \
  --substitutions=\
_ENV=prod,\
_PROJECT_ID=prod-env,\
_REGION=us-central1,\
_REGISTRY=us-central1-docker.pkg.dev/prod-env/mb-repo,\
_SA_EMAIL=mb-pipeline-sa@prod-env.iam.gserviceaccount.com,\
_SOURCE_BUCKET=prod-mb-data-bucket,\
_PIPELINE_BUCKET=prod-mb-pipeline-bucket,\
_COMPOSER_BUCKET=us-central1-prod-mb-composer-XXXX
```

---

## ✅ STEP 6 — Verify Everything Deployed Correctly

Run these checks after a successful build.

```bash
# ── 1. Check Cloud Build history ──────────────────────────────────────────────
# See the last 5 builds and their status (SUCCESS / FAILURE)
gcloud builds list \
  --project=dv-env \
  --limit=5 \
  --format="table(id,status,createTime,duration)"

# ── 2. Check Cloud Run Jobs ───────────────────────────────────────────────────
# All 3 jobs must be listed: dv-mb-ingestion, dv-mb-transform, dv-mb-archive
gcloud run jobs list \
  --region=us-central1 \
  --project=dv-env

# ── 3. Check Docker images in Artifact Registry ───────────────────────────────
# You should see mb-ingestion, mb-transform, mb-archival with :latest and :<SHA> tags
gcloud artifacts docker images list \
  us-central1-docker.pkg.dev/dv-env/mb-repo \
  --include-tags

# ── 4. Check Dataproc scripts uploaded to GCS ─────────────────────────────────
# Must show bronze.py, silver.py, gold.py
gcloud storage ls gs://dv-mb-pipeline-bucket/scripts/

# ── 5. Check Airflow DAG uploaded to Composer bucket ─────────────────────────
# Replace XXXX with your real Composer bucket name
gcloud storage ls gs://us-central1-dv-mb-composer-XXXX/dags/
```

---

## 🔄 STEP 7 — Day-to-Day: Deploying Code Changes

After initial setup, all you need to do to redeploy is **push to the branch**:

```bash
# ── DV deployment (push to coding-branch-daya) ────────────────────────────────
git add .
git commit -m "fix: update transform logic"
git push origin coding-branch-daya
# Cloud Build DV trigger fires automatically ↑

# ── PROD deployment (push to pd-env) ─────────────────────────────────────────
git push origin pd-env
# Cloud Build PROD trigger fires automatically ↑
```

> Cloud Build typically takes **3–6 minutes** to complete all 11 steps.

---

## 🔁 `cloudbuild.yaml` Walkthrough

Here is what each step in the file does:

```yaml
# ── Steps 1–6: Build & push 3 Docker images ──────────────────────────────────
# Each image is tagged with both :latest AND :<SHORT_SHA> (git commit hash)
# SHORT_SHA tagging means you can roll back to any previous commit's image

build-ingestion   → docker build ./ingestion  → tagged :latest + :$SHORT_SHA
push-ingestion    → docker push --all-tags     → sends both tags to Artifact Registry

build-transform   → docker build ./transform
push-transform    → docker push --all-tags

build-archival    → docker build ./archival
push-archival     → docker push --all-tags

# ── Steps 7–9: Deploy Cloud Run Jobs ─────────────────────────────────────────
# Uses :$SHORT_SHA image tag (NOT :latest) — ensures exact version is deployed
# waitFor ensures jobs deploy only after image push succeeds

deploy-ingestion  → gcloud run jobs deploy ${_ENV}-mb-ingestion  --image=...:$SHORT_SHA
deploy-transform  → gcloud run jobs deploy ${_ENV}-mb-transform  --image=...:$SHORT_SHA
deploy-archive    → gcloud run jobs deploy ${_ENV}-mb-archive    --image=...:$SHORT_SHA

# ── Step 10: Sync Dataproc scripts ───────────────────────────────────────────
# Copies all .py files from dataproc_jobs/ to gs://<PIPELINE_BUCKET>/scripts/
sync-scripts      → gcloud storage cp dataproc_jobs/ gs://${_PIPELINE_BUCKET}/scripts/ --recursive

# ── Step 11: Sync Airflow DAG ─────────────────────────────────────────────────
# Copies DAG file to Composer bucket — Composer auto-picks it up within 1-2 min
sync-dag          → gcloud storage cp airflow/ gs://${_COMPOSER_BUCKET}/dags/ --recursive
```

---

## 🔍 Troubleshooting Cloud Build

| Symptom | Cause | Fix |
|---|---|---|
| `PERMISSION_DENIED` on docker push | Cloud Build SA missing Artifact Registry role | Re-run STEP 0 to grant `roles/artifactregistry.writer` |
| `PERMISSION_DENIED` on `run jobs deploy` | Cloud Build SA missing `roles/run.admin` or `roles/iam.serviceAccountUser` | Re-run STEP 0 |
| Build fails at `build-ingestion` | Dockerfile path wrong or syntax error | Confirm `./ingestion/Dockerfile` exists; check Dockerfile syntax |
| `gcloud storage cp` fails | Cloud Build SA missing storage write permission | Grant `roles/storage.objectAdmin` to Cloud Build SA |
| Cloud Run job not updated after push | Trigger not connected / branch pattern wrong | Check trigger: `gcloud builds triggers list` |
| `_COMPOSER_BUCKET` not found error | Placeholder `XXXX` still in substitution | Replace with real Composer bucket name from STEP 2 |
| Build succeeds but old code runs | Airflow is using cached DAG | Wait 2 min for Composer to re-parse; or restart Airflow scheduler |
| Wrong env deployed | Trigger fired on wrong branch | Check `--branch-pattern` regex in trigger definition |

---

## 📋 Build Log Monitoring

```bash
# ── Stream live build logs ────────────────────────────────────────────────────
# Get the latest build ID then stream its logs
BUILD_ID=$(gcloud builds list --project=dv-env --limit=1 --format="value(id)")
gcloud builds log $BUILD_ID --project=dv-env --stream

# ── Check a specific build's status ──────────────────────────────────────────
gcloud builds describe $BUILD_ID --project=dv-env \
  --format="table(status, createTime, finishTime, steps[].status)"
```

---

## 🌐 DV vs PROD Deployment Strategy

| | DV | PROD |
|---|---|---|
| **Branch** | `coding-branch-daya` | `pd-env` |
| **Trigger name** | `mb-pipeline-dv-deploy` | `mb-pipeline-prod-deploy` |
| **GCP Project** | `dv-env` | `prod-env` |
| **Cloud Run Jobs** | `dv-mb-ingestion/transform/archive` | `prod-mb-ingestion/transform/archive` |
| **Airflow DAG ID** | `dv_mobile_brands_pipeline` | `prod_mobile_brands_pipeline` |
| **Deploy frequency** | On every push (active dev) | On merge to `pd-env` (controlled) |
| **Rollback** | `gcloud run jobs update --image=...:$PREV_SHA` | Same |

---

## ↩️ Rolling Back a Bad Deployment

If a new build breaks something, roll back to the previous image SHA:

```bash
# ── Find the previous working image SHA ───────────────────────────────────────
gcloud artifacts docker images list \
  us-central1-docker.pkg.dev/dv-env/mb-repo/mb-ingestion \
  --include-tags \
  --sort-by="~createTime" \
  --limit=5

# ── Roll back Cloud Run Job to previous SHA ───────────────────────────────────
# Replace PREV_SHA with the SHA tag of the last working image
gcloud run jobs update dv-mb-ingestion \
  --image=us-central1-docker.pkg.dev/dv-env/mb-repo/mb-ingestion:PREV_SHA \
  --region=us-central1 \
  --project=dv-env

# Repeat for transform and archive if needed
gcloud run jobs update dv-mb-transform \
  --image=us-central1-docker.pkg.dev/dv-env/mb-repo/mb-transform:PREV_SHA \
  --region=us-central1 --project=dv-env

gcloud run jobs update dv-mb-archive \
  --image=us-central1-docker.pkg.dev/dv-env/mb-repo/mb-archival:PREV_SHA \
  --region=us-central1 --project=dv-env
```

---

## ✅ Final Summary

```
One-time setup (Steps 0–3):
  ✔ Grant Cloud Build SA permissions
  ✔ Create Artifact Registry repo
  ✔ Find Composer bucket name
  ✔ Connect GitHub repo

One-time trigger creation (Step 4):
  ✔ DV trigger  → branch: coding-branch-daya
  ✔ PROD trigger → branch: pd-env

First deployment (Step 5):
  ✔ gcloud builds submit --config=cloudbuild.yaml ...

After that — just push code:
  git push origin coding-branch-daya   → DV auto-deploys
  git push origin pd-env               → PROD auto-deploys
```