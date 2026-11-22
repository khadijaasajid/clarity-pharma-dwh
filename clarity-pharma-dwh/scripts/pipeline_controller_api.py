

"""
Pipeline Controller API
========================
FastAPI service that automates the full Clarity Pharma DWH pipeline.
Hit /pipeline/reset-and-automate to wipe old data and run:
  baseline (immediate) -> batch_1 (after 3 min) -> batch_2 (after 5 min)

All 3 waves go through dag_00 -> dag_01 -> dag_02 -> dag_03 chain.
No independent dag_01 triggers, so no data overlap.

Swagger UI: http://localhost:8000/docs
"""

from fastapi import FastAPI, BackgroundTasks
from sqlalchemy import create_engine, text
import requests
import time
import os
from datetime import datetime

app = FastAPI(title="Clarity Pharma Pipeline Controller API")

# Connection settings inside Docker network
STAGING_CONN = os.environ.get(
    "STAGING_DB_CONN",
    "postgresql+psycopg2://staging:staging@postgres-staging:5432/staging"
)
AIRFLOW_BASE = os.environ.get("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_AUTH = ("admin", "admin")

EXPORT_DIR = "/opt/airflow/data/exports"
DUCKDB_PATH = "/opt/airflow/duckdb/clarity_pharma_sdi.duckdb"

PIPELINE_STATE = {
    "status": "IDLE",
    "current_wave": None,
    "started_at": None,
    "history": []
}


def reset_databases():
    """Drop staging and metric tables in PostgreSQL, remove old exports and DuckDB."""
    engine = create_engine(STAGING_CONN)
    tables = [
        "master_data", "pharmacy_sales", "hospital_demand", "iot_streaming",
        "ingestion_metrics", "streaming_metrics", "storage_metrics", "query_performance_log"
    ]
    with engine.begin() as conn:
        for t in tables:
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t}"))

    # Remove old metric CSVs
    for f in ["ingestion_metrics.csv", "streaming_metrics.csv", "storage_metrics.csv", "query_performance_log.csv"]:
        fpath = os.path.join(EXPORT_DIR, f)
        if os.path.exists(fpath):
            os.remove(fpath)

    # Remove old DuckDB file
    if os.path.exists(DUCKDB_PATH):
        os.remove(DUCKDB_PATH)


def trigger_airflow_dag(dag_id, conf):
    """Trigger an Airflow DAG via REST API."""
    url = f"{AIRFLOW_BASE}/dags/{dag_id}/dagRuns"
    resp = requests.post(url, json={"conf": conf}, auth=AIRFLOW_AUTH, timeout=15)
    resp.raise_for_status()
    return resp.json().get("dag_run_id")


def run_timed_sequence(batch1_delay_sec, batch2_delay_sec):
    """Background worker: baseline (now) -> batch_1 (+delay) -> batch_2 (+delay after batch_1)."""
    try:
        # Wave 0: Baseline (through dag_00 with passthrough)
        PIPELINE_STATE["status"] = "RUNNING"
        PIPELINE_STATE["current_wave"] = "Wave 0: baseline (2023 to 2025)"
        run_id = trigger_airflow_dag("dag_00_data_generator", {"batch_folder": "baseline"})
        PIPELINE_STATE["history"].append(
            f"[{datetime.now().strftime('%H:%M:%S')}] Triggered baseline ({run_id})"
        )

        # Wait for batch_1
        time.sleep(batch1_delay_sec)

        # Wave 1: Generate and ingest batch_1
        PIPELINE_STATE["current_wave"] = "Wave 1: batch_1 (Jan to Jun 2026)"
        run_id = trigger_airflow_dag("dag_00_data_generator", {"batch_num": 1})
        PIPELINE_STATE["history"].append(
            f"[{datetime.now().strftime('%H:%M:%S')}] Triggered batch_1 generation ({run_id})"
        )

        # Wait for batch_2
        time.sleep(batch2_delay_sec)

        # Wave 2: Generate and ingest batch_2
        PIPELINE_STATE["current_wave"] = "Wave 2: batch_2 (Jul to Dec 2026)"
        run_id = trigger_airflow_dag("dag_00_data_generator", {"batch_num": 2})
        PIPELINE_STATE["history"].append(
            f"[{datetime.now().strftime('%H:%M:%S')}] Triggered batch_2 generation ({run_id})"
        )

        PIPELINE_STATE["status"] = "COMPLETED"
        PIPELINE_STATE["current_wave"] = "All 3 waves triggered"

    except Exception as e:
        PIPELINE_STATE["status"] = f"ERROR: {str(e)}"
        PIPELINE_STATE["history"].append(
            f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: {str(e)}"
        )


@app.get("/")
def root():
    """API home."""
    return {
        "name": "Clarity Pharma Pipeline Controller",
        "endpoints": {
            "POST /pipeline/reset-and-automate": "Wipe and run full pipeline",
            "GET /pipeline/status": "Check current pipeline status",
            "POST /pipeline/trigger/{dag_id}": "Trigger a specific DAG",
        }
    }


@app.get("/pipeline/status")
def get_status():
    """Check which wave is currently running."""
    return PIPELINE_STATE


@app.post("/pipeline/reset-and-automate")
def start_automated_pipeline(
    background_tasks: BackgroundTasks,
    batch1_delay_minutes: int = 3,
    batch2_delay_minutes: int = 5
):
    """
    Full automated pipeline:
    1. Wipes old staging tables, metrics, and DuckDB
    2. Runs baseline immediately (dag_00 passthrough -> dag_01 -> dag_02 -> dag_03)
    3. Generates and ingests batch_1 after batch1_delay_minutes
    4. Generates and ingests batch_2 after batch2_delay_minutes (from batch_1)
    """
    reset_databases()
    PIPELINE_STATE["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    PIPELINE_STATE["status"] = "STARTING"
    PIPELINE_STATE["history"] = [
        f"[{datetime.now().strftime('%H:%M:%S')}] Databases wiped clean"
    ]

    background_tasks.add_task(
        run_timed_sequence,
        batch1_delay_minutes * 60,
        batch2_delay_minutes * 60
    )

    return {
        "message": "Automated pipeline started!",
        "schedule": {
            "wave_0_baseline": "Immediate (0 min)",
            "wave_1_batch_1": f"In {batch1_delay_minutes} minutes",
            "wave_2_batch_2": f"In {batch1_delay_minutes + batch2_delay_minutes} minutes",
        },
        "monitor": "GET /pipeline/status"
    }


@app.post("/pipeline/trigger/{dag_id}")
def trigger_single_dag(dag_id: str, batch_folder: str = "baseline"):
    """Trigger a specific DAG manually."""
    try:
        run_id = trigger_airflow_dag(dag_id, {"batch_folder": batch_folder})
        return {"status": "triggered", "dag_id": dag_id, "run_id": run_id}
    except Exception as e:
        return {"status": "error", "message": str(e)}