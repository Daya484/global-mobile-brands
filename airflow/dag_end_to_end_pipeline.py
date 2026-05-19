"""
airflow/dag_mb_pipeline.py
--------------------------------------------------
Pipeline:

1. Generator (Cloud Run)
2. Transformer (Cloud Run)
3. Dataproc Bronze → Silver → Gold
4. Archival (Cloud Run)
5. Delete cluster (always)

✅ Optimized & corrected version
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.trigger_rule import TriggerRule


# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

ENV        = Variable.get("MB_ENV", "dv")
PROJECT_ID = Variable.get("MB_PROJECT_ID", "dv-env")
REGION     = Variable.get("MB_REGION", "us-central1")

PIPELINE_BUCKET = Variable.get("MB_PIPELINE_BUCKET", "dv-mb-pipeline-bucket")
SOURCE_BUCKET   = Variable.get("MB_SOURCE_BUCKET",   "dv-mb-data-bucket")   # holds landing/ & transformed/
BQ_DATASET      = Variable.get("MB_BQ_DATASET", "mobile_brands")

CLUSTER_NAME = f"{ENV}-mb-cluster-{{{{ ds_nodash }}}}"
SCRIPTS_URI  = f"gs://{PIPELINE_BUCKET}/scripts"

# -----------------------------------------------------------------------------
# DATAPROC CONFIG
# -----------------------------------------------------------------------------

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
    },
    "secondary_worker_config": {
        "num_instances": 2,
        "preemptibility": "PREEMPTIBLE",
    },
    "software_config": {
        # 2.2-debian12 = Spark 3.5 — required for Delta Lake 3.x compatibility
        "image_version": "2.2-debian12",
        "properties": {
            # Delta Lake — loaded at cluster level so all spark-submit jobs pick it up
            "spark:spark.jars.packages":
                "io.delta:delta-spark_2.12:3.2.0",
            "spark:spark.sql.extensions":
                "io.delta.sql.DeltaSparkSessionExtension",
            "spark:spark.sql.catalog.spark_catalog":
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # Performance
            "spark:spark.sql.adaptive.enabled": "true",
            "spark:spark.sql.shuffle.partitions": "100",
        },
    },
}


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

def pyspark_job(script):
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{SCRIPTS_URI}/{script}",
            "args": [
                "--dt={{ ds }}",
                "--run_id={{ run_id }}",
                f"--pipeline_bucket={PIPELINE_BUCKET}",
                f"--source_bucket={SOURCE_BUCKET}",   # for bronze: where transformed/ CSVs live
                f"--project_id={PROJECT_ID}",
                f"--dataset={BQ_DATASET}",
                f"--env={ENV}",
            ],
        },
    }


def cloud_run_env():
    """
    Env vars injected into every Cloud Run job execution.
    BUCKET_NAME = SOURCE_BUCKET because ingestion/transform/archival all
    read and write the data bucket (landing/, transformed/, archive_*/).
    """
    return {
        "containerOverrides": [{
            "env": [
                {"name": "RUN_DATE",    "value": "{{ ds }}"},
                {"name": "BUCKET_NAME", "value": SOURCE_BUCKET},
            ]
        }]
    }


# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=f"{ENV}_mobile_brands_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["mobile-brands"],
) as dag:

    # -----------------------------------------------------------------------------
    # 1. GENERATE FILES (Cloud Run)
    # -----------------------------------------------------------------------------
    t_generate = CloudRunExecuteJobOperator(
        task_id="generate_excel",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=f"{ENV}-generator",
        overrides=cloud_run_env(),
    )

    # -----------------------------------------------------------------------------
    # 2. TRANSFORM FILES
    # -----------------------------------------------------------------------------
    t_transform = CloudRunExecuteJobOperator(
        task_id="transform_csv",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=f"{ENV}-transform",
        overrides=cloud_run_env(),
    )

    # -----------------------------------------------------------------------------
    # 3. CREATE CLUSTER
    # -----------------------------------------------------------------------------
    t_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    # -----------------------------------------------------------------------------
    # 4. BRONZE → SILVER → GOLD
    # -----------------------------------------------------------------------------

    t_bronze = DataprocSubmitJobOperator(
        task_id="bronze",
        job=pyspark_job("bronze.py"),
        project_id=PROJECT_ID,
        region=REGION,
    )

    t_silver = DataprocSubmitJobOperator(
        task_id="silver",
        job=pyspark_job("silver.py"),
        project_id=PROJECT_ID,
        region=REGION,
    )

    t_gold = DataprocSubmitJobOperator(
        task_id="gold",
        job=pyspark_job("gold.py"),
        project_id=PROJECT_ID,
        region=REGION,
    )

    # -----------------------------------------------------------------------------
    # 5. ARCHIVE
    # -----------------------------------------------------------------------------
    t_archive = CloudRunExecuteJobOperator(
        task_id="archive",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=f"{ENV}-archive",
        overrides=cloud_run_env(),
    )

    # -----------------------------------------------------------------------------
    # 6. DELETE CLUSTER (ALWAYS)
    # -----------------------------------------------------------------------------
    t_delete = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # -----------------------------------------------------------------------------
    # FLOW
    # -----------------------------------------------------------------------------

    t_generate >> t_transform >> t_cluster
    t_cluster >> t_bronze >> t_silver >> t_gold

    # Archive completes first, then cluster is deleted (trigger_rule=ALL_DONE
    # on t_delete ensures cleanup even if archive fails)
    t_gold >> t_archive >> t_delete