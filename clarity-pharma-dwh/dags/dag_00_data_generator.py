

"""
DAG 00 - Data Oversampler & Generator
======================================
Generates synthetic oversampled data across all 7 Clarity Pharma source files
to simulate a live 24/7 pipeline. Outputs batch_1 (H1 2026: Jan to Jun) and
batch_2 (H2 2026: Jul to Dec) as .xlsx files (01-06) and .csv (07_iot),
preserving distributions, ID sequences, and cross-file foreign keys.

Trigger via Airflow UI with config:
  {"batch_num": 1}             -> generates batch_1 (H1 2026: Jan to Jun)
  {"batch_num": 2}             -> generates batch_2 (H2 2026: Jul to Dec)
  {"batch_folder": "baseline"} -> skips generation, passes baseline to dag_01

Or leave empty to generate both batches sequentially.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import os
import sys
import json

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
DATA_DIR = "/opt/airflow/data"
SCRIPTS_DIR = "/opt/airflow/scripts"

default_args = {
    "owner": "clarity_pharma",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="dag_00_data_generator",
    default_args=default_args,
    description="Generate oversampled synthetic data for all 7 source files",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sdi", "oversampling", "data_gen"],
)


# ══════════════════════════════════════════════
# Task Functions
# ══════════════════════════════════════════════

def generate_batch_data(**context):
    """
    Call generate_data.generate_batch() for the requested batch number.
    Reads batch_num from dag_run.conf, defaults to generating both batches.
    If batch_folder is 'baseline', skips generation and passes through.
    """
    conf = context.get("dag_run").conf or {}

    # Baseline passthrough: skip generation, just trigger dag_01
    batch_folder = conf.get("batch_folder", None)
    if batch_folder == "baseline":
        print("Baseline mode: skipping generation, passing through to ingestion.")
        context["ti"].xcom_push(key="batch_folder", value="baseline")
        return

    # Add scripts dir to path so we can import generate_data
    if SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, SCRIPTS_DIR)

    from generate_data import generate_batch

    batch_num = conf.get("batch_num", None)

    if batch_num:
        # Generate a single batch
        batch_num = int(batch_num)
        print(f"Generating batch_{batch_num}...")
        report = generate_batch(batch_num)
        print(f"\n=== Generated {report['batch']} ({report['period']}) ===")
        for item in report["files"]:
            print(f"  {item['file']:35s} | +{item['generated_rows']:,} rows")
        all_reports = [report]
    else:
        # Generate both batches
        all_reports = []
        for b in [1, 2]:
            print(f"\nGenerating batch_{b}...")
            report = generate_batch(b)
            all_reports.append(report)
            print(f"=== Generated {report['batch']} ({report['period']}) ===")
            for item in report["files"]:
                print(f"  {item['file']:35s} | +{item['generated_rows']:,} rows")

    # Save summary JSON
    report_path = os.path.join(DATA_DIR, "oversampling_summary.json")
    with open(report_path, "w") as f:
        json.dump(all_reports, f, indent=2)
    print(f"\nSaved oversampling summary to {report_path}")

    # Push batch_folder to XCom for downstream DAGs
    if batch_num:
        context["ti"].xcom_push(key="batch_folder", value=f"batch_{batch_num}")
    else:
        context["ti"].xcom_push(key="batch_folder", value="batch_1")


def verify_generated_files(**context):
    """Verify that all expected output files exist and report sizes."""
    conf = context.get("dag_run").conf or {}

    # Baseline passthrough: verify baseline folder instead
    batch_folder = conf.get("batch_folder", None)
    if batch_folder == "baseline":
        baseline_dir = os.path.join(DATA_DIR, "baseline")
        print(f"\n=== Verifying baseline at {baseline_dir} ===")
        if os.path.exists(baseline_dir):
            files = os.listdir(baseline_dir)
            for fname in sorted(files):
                fpath = os.path.join(baseline_dir, fname)
                size_mb = os.path.getsize(fpath) / (1024 * 1024)
                print(f"  OK: {fname} ({size_mb:.2f} MB)")
            print(f"\nBaseline folder verified: {len(files)} files found.")
        else:
            raise FileNotFoundError(f"Baseline folder not found at {baseline_dir}")
        return

    batch_num = conf.get("batch_num", None)
    batches = [int(batch_num)] if batch_num else [1, 2]

    xlsx_files = [
        "01_master_data.xlsx",
        "02_manufacturing_data.xlsx",
        "03_manufacturer_delivery_data.xlsx",
        "04_warehouse_data.xlsx",
        "05_pharmacy_sales_data.xlsx",
        "06_hospital_demand_data.xlsx",
    ]
    csv_files = ["07_iot_streaming_data.csv"]

    all_ok = True
    for b in batches:
        batch_dir = os.path.join(DATA_DIR, f"batch_{b}")
        print(f"\n=== Verifying batch_{b} at {batch_dir} ===")

        for fname in xlsx_files + csv_files:
            fpath = os.path.join(batch_dir, fname)
            if os.path.exists(fpath):
                size_mb = os.path.getsize(fpath) / (1024 * 1024)
                print(f"  OK: {fname} ({size_mb:.2f} MB)")
            else:
                print(f"  MISSING: {fname}")
                all_ok = False

    if not all_ok:
        raise FileNotFoundError("Some generated files are missing!")
    print("\nAll generated files verified successfully.")


# ══════════════════════════════════════════════
# Task Definitions
# ══════════════════════════════════════════════

t_generate = PythonOperator(
    task_id="generate_batch_data",
    python_callable=generate_batch_data,
    provide_context=True,
    dag=dag,
)

t_verify = PythonOperator(
    task_id="verify_generated_files",
    python_callable=verify_generated_files,
    provide_context=True,
    dag=dag,
)

t_trigger_ingestion = TriggerDagRunOperator(
    task_id="trigger_dag_01_ingestion",
    trigger_dag_id="dag_01_batch_ingestion",
    conf={"batch_folder": "{{ ti.xcom_pull(task_ids='generate_batch_data', key='batch_folder') }}"},
    dag=dag,
)

# ══════════════════════════════════════════════
# Dependencies
# ══════════════════════════════════════════════
# generate → verify → trigger dag_01

t_generate >> t_verify >> t_trigger_ingestion