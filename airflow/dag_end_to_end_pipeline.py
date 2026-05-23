"""
✅ Mobile Brands Pipeline (Serverless + Email Alerts)

--------------------------------------------------
FLOW:

START
 ↓
Cloud Run → Generator (Ingestion)
 ↓
Cloud Run → Transform
 ↓
Dataproc Serverless → Bronze
 ↓
Dataproc Serverless → Silver
 ↓
Dataproc Serverless → Gold
 ↓
Cloud Run → Archive
 ↓
✅ SUCCESS EMAIL
 ↓
END

✅ FAILURE EMAIL (automatic via callback)
--------------------------------------------------
"""

# -----------------------------------------------------------------------------
# IMPORTS
# -----------------------------------------------------------------------------

from datetime import datetime, timedelta
import time

import logging  # ✅ Standard logging (FIXED)

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.trigger_rule import TriggerRule


# -----------------------------------------------------------------------------
# LOGGER (✅ FIXED FROM LoggingMixin)
# -----------------------------------------------------------------------------
log = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# CONFIG (Dynamic via Airflow Variables)
# -----------------------------------------------------------------------------

# ✅ Environment (dv / pd)
ENV = Variable.get("MB_ENV", "dv")

# ✅ Project details
PROJECT_ID = Variable.get("MB_PROJECT_ID", "dev-env-496908")
REGION = Variable.get("MB_REGION", "us-central1")

# ✅ Buckets & dataset
PIPELINE_BUCKET = Variable.get("MB_PIPELINE_BUCKET", "mb-pipeline-dev-496908")
SOURCE_BUCKET = Variable.get("MB_SOURCE_BUCKET", "mb-data-dev-496908")
BQ_DATASET = Variable.get("MB_BQ_DATASET", "mobile_brands")

# ✅ Scripts location (Bronze/Silver/Gold pyspark scripts)
SCRIPTS_URI = f"gs://{PIPELINE_BUCKET}/scripts"

# ✅ Alert email
EMAIL = "dayasagarreddy2943@gmail.com"


# -----------------------------------------------------------------------------
# FAILURE EMAIL CALLBACK
# -----------------------------------------------------------------------------

def send_failure_email(context):
    """
    ✅ Sends email automatically when ANY task fails
    """
    log.error("Task failed! Sending failure email...")

    EmailOperator(
        task_id="send_failure_email",
        to=[EMAIL],
        subject=f"🚨 Airflow FAILURE: {context['task_instance'].task_id}",
        html_content=f"""
        <h3>🚨 Task Failed</h3>
        <b>DAG:</b> {context['dag'].dag_id}<br>
        <b>Task:</b> {context['task_instance'].task_id}<br>
        <b>Date:</b> {context.get('logical_date', context.get('data_interval_start', 'N/A'))}<br>
        <b><a href="{context['task_instance'].log_url}">View Logs</a></b>
        """,
    ).execute(context=context)


# -----------------------------------------------------------------------------
# DEFAULT DAG ARGUMENTS
# -----------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "retries": 0,  # ✅ default no retry
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_failure_email,  # ✅ auto failure email
}


# -----------------------------------------------------------------------------
# HELPER: Cloud Run Environment Variables
# -----------------------------------------------------------------------------

def cloud_run_env():
    """
    ✅ Pass dynamic values to Cloud Run jobs
    """
    log.info("Setting Cloud Run environment variables")

    return {
        "containerOverrides": [{
            "env": [
                {"name": "RUN_DATE", "value": "{{ ds }}"},
                {"name": "BUCKET_NAME", "value": SOURCE_BUCKET},
                {"name": "ENV", "value": ENV},
            ]
        }]
    }


# -----------------------------------------------------------------------------
# HELPER: Dataproc Serverless Batch Config
# -----------------------------------------------------------------------------

def dataproc_batch(script):
    """
    ✅ Creates Dataproc Serverless job for given script
    ✅ Resource limits set to stay within free-tier quota
    """
    log.info(f"Preparing Dataproc batch for {script}")

    return {
        "pyspark_batch": {
            "main_python_file_uri": f"{SCRIPTS_URI}/{script}",
            "args": [
                "--dt={{ ds }}",  # execution date
                "--run_id={{ run_id }}",
                f"--pipeline_bucket={PIPELINE_BUCKET}",
                f"--source_bucket={SOURCE_BUCKET}",
                f"--project_id={PROJECT_ID}",
                f"--dataset={BQ_DATASET}",
                f"--env={ENV}",
            ],
        },
        "runtime_config": {
            "properties": {
                # ✅ Dataproc Serverless constraints:
                # - executor cores must be 4, 8, or 16 (not 2)
                # - initialExecutors must be >= 2
                # - driver memory min = cores × 1024mb (4 cores = min 4g)
                "spark.executor.cores": "4",              # minimum allowed
                "spark.dynamicAllocation.initialExecutors": "2",  # minimum allowed
                "spark.dynamicAllocation.maxExecutors": "2",      # keep low
                "spark.executor.memory": "4g",            # min for 4 cores
                "spark.driver.memory": "4g",              # min for 4 cores
            }
        },
        "environment_config": {
            "execution_config": {
                "service_account": f"mb-pipeline-sa@{PROJECT_ID}.iam.gserviceaccount.com"
            }
        }
    }


# -----------------------------------------------------------------------------
# DAG DEFINITION
# -----------------------------------------------------------------------------

with DAG(
    dag_id=f"{ENV}_mobile_brands_pipeline_serverless",
    start_date=datetime(2024, 1, 1),
    schedule="0 1 * * *",   # ✅ CORRECT # ✅ Daily at 1 AM
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["mobile-brands", "serverless"],
) as dag:

    # -------------------------------------------------------------------------
    # START TASK
    # -------------------------------------------------------------------------
    start = EmptyOperator(task_id="start")

    # -------------------------------------------------------------------------
    # CLOUD RUN: GENERATE (RETRY ENABLED)
    # -------------------------------------------------------------------------
    t_generate = CloudRunExecuteJobOperator(
        task_id="generate_excel",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="mb-ingestion-job",
        #overrides=cloud_run_env(),
        retries=3,  # ✅ retry because external dependency
        retry_delay=timedelta(minutes=2),
    )

    # -------------------------------------------------------------------------
    # CLOUD RUN: TRANSFORM (RETRY ENABLED)
    # -------------------------------------------------------------------------
    t_transform = CloudRunExecuteJobOperator(
        task_id="transform_csv",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="mb-transform-job", 
        #overrides=cloud_run_env(),
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    # -------------------------------------------------------------------------
    # DATAPROC: BRONZE
    # -------------------------------------------------------------------------
    t_bronze = DataprocCreateBatchOperator(
        task_id="bronze",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="bronze-{{ ds }}",  # ✅ e.g. bronze-2026-05-23
        batch=dataproc_batch("bronze.py"),
    )

    # -------------------------------------------------------------------------
    # DATAPROC: SILVER
    # -------------------------------------------------------------------------
    t_silver = DataprocCreateBatchOperator(
        task_id="silver",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="silver-{{ ds }}",  # ✅ e.g. silver-2026-05-23
        batch=dataproc_batch("silver.py"),
    )

    # -------------------------------------------------------------------------
    # DATAPROC: GOLD (NO RETRY - HEAVY JOB)
    # -------------------------------------------------------------------------
    t_gold = DataprocCreateBatchOperator(
        task_id="gold",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="gold-{{ ds }}",  # ✅ e.g. gold-2026-05-23
        batch=dataproc_batch("gold.py"),
        retries=0
    )

    # -------------------------------------------------------------------------
    # CLOUD RUN: ARCHIVE
    # -------------------------------------------------------------------------
    t_archive = CloudRunExecuteJobOperator(
        task_id="archive",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="mb-archival-job", 
        #overrides=cloud_run_env(),
    )

    # -------------------------------------------------------------------------
    # SUCCESS EMAIL (ONLY IF ALL TASKS PASS)
    # -------------------------------------------------------------------------
    t_success = EmailOperator(
        task_id="success_email",
        to=[EMAIL],
        subject="✅ Airflow Pipeline SUCCESS",
        html_content="<h3>✅ Pipeline completed successfully</h3>",
    )

    # -------------------------------------------------------------------------
    # QUOTA RELEASE WAIT (GCP takes ~2 min to release CPU quota after batch)
    # -------------------------------------------------------------------------
    wait_after_bronze = PythonOperator(
        task_id="wait_after_bronze",
        python_callable=lambda: (log.info("Waiting 120s for GCP quota release after bronze..."), time.sleep(120)),
    )

    wait_after_silver = PythonOperator(
        task_id="wait_after_silver",
        python_callable=lambda: (log.info("Waiting 120s for GCP quota release after silver..."), time.sleep(120)),
    )

    # -------------------------------------------------------------------------
    # END TASK (ALWAYS RUN EVEN IF FAILURE)
    # -------------------------------------------------------------------------
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE
    )

    # -------------------------------------------------------------------------
    # EXECUTION FLOW (with quota wait between Dataproc jobs)
    # -------------------------------------------------------------------------
    start >> t_generate >> t_transform >> t_bronze >> wait_after_bronze >> t_silver >> wait_after_silver >> t_gold >> t_archive >> t_success >> end