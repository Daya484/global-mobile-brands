"""
airflow/dag_end_to_end_pipeline.py — Mobile Brands End-to-End Pipeline
=======================================================================
Full orchestration DAG:

  ingestion (Cloud Run Job)
      ↓
  transform (Cloud Run Job)
      ↓
  create_dataproc_cluster (ephemeral)
      ↓
  bronze → silver → gold  (Dataproc PySpark jobs)
      ↓
  delete_dataproc_cluster  (always runs, even on failure)
      ↓
  archival (Cloud Run Job)

Best practices applied:
  ✅ Idempotent paths (dt + run_id partition)
  ✅ Ephemeral Dataproc cluster (auto-deleted)
  ✅ Preemptible workers for cost saving
  ✅ Data Quality gate inside bronze.py (fail fast)
  ✅ Cluster always deleted (TriggerRule.ALL_DONE)
  ✅ Retry logic on all tasks
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Config from Airflow Variables ─────────────────────────────────────────────
ENV             = Variable.get("MB_ENV",             default_var="dv")
PROJECT_ID      = Variable.get("MB_PROJECT_ID",      default_var="dv-env")
REGION          = Variable.get("MB_REGION",          default_var="us-central1")
PIPELINE_BUCKET = Variable.get("MB_PIPELINE_BUCKET", default_var="dv-mb-pipeline-bucket")
SOURCE_BUCKET   = Variable.get("MB_SOURCE_BUCKET",   default_var="dv-mobile-brands")
BQ_DATASET      = Variable.get("MB_BQ_DATASET",      default_var="mobile_brands")

CLUSTER_NAME    = f"{ENV}-mb-cluster-{{{{ ds_nodash }}}}"
SCRIPTS_URI     = f"gs://{PIPELINE_BUCKET}/scripts"

# ── Ephemeral Dataproc cluster config ────────────────────────────────────────
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 100},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "secondary_worker_config": {   # preemptible workers (cost saving)
        "num_instances": 2,
        "is_preemptible": True,
        "preemptibility": "PREEMPTIBLE",
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "spark:spark.sql.adaptive.enabled":                    "true",
            "spark:spark.sql.sources.partitionOverwriteMode":      "dynamic",
        },
    },
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 3600},
    },
}


def _pyspark_job(script: str) -> dict:
    """Build a DataprocSubmitJobOperator job dict for the given PySpark script."""
    return {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{SCRIPTS_URI}/{script}",
            "args": [
                "--dt={{ ds }}",
                "--run_id={{ run_id }}",
                f"--pipeline_bucket={PIPELINE_BUCKET}",
                f"--project_id={PROJECT_ID}",
                f"--bq_dataset={BQ_DATASET}",
                f"--env={ENV}",
            ],
        },
    }


def _cloud_run_job_overrides(job_name: str) -> dict:
    """Build env-var overrides for a Cloud Run Job execution."""
    return {
        "containerOverrides": [
            {
                "env": [
                    {"name": "SOURCE_BUCKET",   "value": SOURCE_BUCKET},
                    {"name": "DEST_BUCKET",     "value": PIPELINE_BUCKET},
                    {"name": "RUN_ID",          "value": "{{ run_id }}"},
                    {"name": "DT",              "value": "{{ ds }}"},
                    {"name": "LOG_LEVEL",       "value": "INFO"},
                ]
            }
        ]
    }


# ── DAG ───────────────────────────────────────────────────────────────────────
default_args = {
    "owner":             "data-engineering",
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
    "depends_on_past":   False,
}

with DAG(
    dag_id=f"{ENV}_mb_end_to_end_pipeline",
    description="Mobile Brands: ingestion → transform → Dataproc (bronze/silver/gold) → archival",
    schedule_interval="0 1 * * *",    # 01:00 UTC = 06:30 IST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["mobile-brands", "medallion", "dataproc", "cloud-run", ENV],
) as dag:

    # 1. Ingestion — Cloud Run Job ─────────────────────────────────────────────
    t_ingestion = CloudRunExecuteJobOperator(
        task_id="ingestion",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=f"{ENV}-mb-ingestion",
        overrides=_cloud_run_job_overrides("ingestion"),
        deferrable=False,
    )

    # 2. Transform — Cloud Run Job ─────────────────────────────────────────────
    t_transform = CloudRunExecuteJobOperator(
        task_id="transform",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=f"{ENV}-mb-transform",
        overrides=_cloud_run_job_overrides("transform"),
        deferrable=False,
    )

    # 3. Create ephemeral Dataproc cluster ────────────────────────────────────
    t_create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config=CLUSTER_CONFIG,
    )

    # 4. Bronze ───────────────────────────────────────────────────────────────
    t_bronze = DataprocSubmitJobOperator(
        task_id="bronze",
        job=_pyspark_job("bronze.py"),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # 5. Silver ───────────────────────────────────────────────────────────────
    t_silver = DataprocSubmitJobOperator(
        task_id="silver",
        job=_pyspark_job("silver.py"),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # 6. Gold ─────────────────────────────────────────────────────────────────
    t_gold = DataprocSubmitJobOperator(
        task_id="gold",
        job=_pyspark_job("gold.py"),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # 7. Delete cluster — ALWAYS runs even on failure ──────────────────────────
    t_delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # 8. Archival — Cloud Run Job ──────────────────────────────────────────────
    t_archival = CloudRunExecuteJobOperator(
        task_id="archival",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=f"{ENV}-mb-archival",
        overrides=_cloud_run_job_overrides("archival"),
        deferrable=False,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────
    t_ingestion >> t_transform >> t_create_cluster
    t_create_cluster >> t_bronze >> t_silver >> t_gold
    t_gold >> t_delete_cluster >> t_archival
