"""
DAG 01 - Batch Ingestion (with FileSensor + Excel Support)
===========================================================
Watches for source files using FileSensor, then loads them into PostgreSQL
staging tables. Supports both .xlsx (batch files 01-06) and .csv (baseline
and 07_iot_streaming). Logs ingestion + streaming + storage metrics for
DE dashboards. Auto syncs metrics CSVs into PostgreSQL for Grafana.

Trigger via Airflow UI with config:
  {"batch_folder": "batch_1"}   -> ingest from data/batch_1/
  {"batch_folder": "batch_2"}   -> ingest from data/batch_2/
  {"batch_folder": "baseline"}  -> ingest from data/baseline/ (original CSVs)

Source Files (SDI mart):
  - 01_master_data (.xlsx/.csv)
  - 05_pharmacy_sales_data (.xlsx/.csv)
  - 06_hospital_demand_data (.xlsx/.csv)
  - 07_iot_streaming_data (.csv) — streaming ingestion with dirty row detection
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import time
import glob
from sqlalchemy import create_engine

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
DATA_DIR = "/opt/airflow/data"
METRICS_DIR = "/opt/airflow/data/exports"
STAGING_CONN = os.environ.get(
    "STAGING_DB_CONN",
    "postgresql+psycopg2://staging:staging@postgres-staging:5432/staging"
)

# SDI files we ingest (stems without extension)
SDI_BATCH_FILES = ["01_master_data", "05_pharmacy_sales_data", "06_hospital_demand_data"]
SDI_STREAM_FILE = "07_iot_streaming_data"

default_args = {
    "owner": "clarity_pharma",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="dag_01_batch_ingestion",
    default_args=default_args,
    description="FileSensor + Excel/CSV ingestion into PostgreSQL staging",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sdi", "extract", "staging", "ingestion"],
)


# ══════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════

def get_batch_folder(**context):
    """Resolve batch folder from dag_run.conf or Airflow Variable."""
    conf = context.get("dag_run").conf or {}
    folder = conf.get(
        "batch_folder",
        Variable.get("current_batch", default_var="batch_1")
    )
    return folder


def resolve_filepath(batch_folder, file_stem):
    """Find the file as .xlsx first, then .csv. Returns full path or None."""
    base = os.path.join(DATA_DIR, batch_folder)
    xlsx_path = os.path.join(base, f"{file_stem}.xlsx")
    csv_path = os.path.join(base, f"{file_stem}.csv")

    if os.path.exists(xlsx_path):
        return xlsx_path
    elif os.path.exists(csv_path):
        return csv_path
    return None


def smart_read(filepath):
    """Read .xlsx or .csv transparently."""
    if filepath.endswith(".xlsx"):
        return pd.read_excel(filepath, engine="openpyxl")
    else:
        return pd.read_csv(filepath, low_memory=False)


def log_metric(metric_type, data_dict):
    """Append a row to the appropriate metrics CSV."""
    os.makedirs(METRICS_DIR, exist_ok=True)
    filename = f"{metric_type}_metrics.csv"
    metrics_file = os.path.join(METRICS_DIR, filename)

    data_dict["MeasuredAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_dict["RunDate"] = datetime.now().strftime("%Y-%m-%d")

    df = pd.DataFrame([data_dict])
    if os.path.exists(metrics_file):
        df.to_csv(metrics_file, mode="a", header=False, index=False)
    else:
        df.to_csv(metrics_file, index=False)


# ══════════════════════════════════════════════
# Task: Check source files exist (FileSensor fallback)
# ══════════════════════════════════════════════

def check_source_files(**context):
    """
    Verify all SDI source files exist in the target batch folder.
    Acts as a programmatic FileSensor with detailed logging.
    """
    batch_folder = get_batch_folder(**context)
    batch_path = os.path.join(DATA_DIR, batch_folder)
    context["ti"].xcom_push(key="batch_folder", value=batch_folder)

    print(f"Checking source files in: {batch_path}")

    missing = []
    for stem in SDI_BATCH_FILES + [SDI_STREAM_FILE]:
        fpath = resolve_filepath(batch_folder, stem)
        if fpath:
            size_mb = os.path.getsize(fpath) / (1024 * 1024)
            ext = os.path.splitext(fpath)[1]
            print(f"  Found: {stem}{ext} ({size_mb:.2f} MB)")
        else:
            print(f"  MISSING: {stem} (.xlsx or .csv)")
            missing.append(stem)

    if missing:
        raise FileNotFoundError(
            f"Missing files in {batch_path}: {missing}"
        )
    print(f"All SDI source files present in {batch_folder}.")


# ══════════════════════════════════════════════
# Task: Load batch files into PostgreSQL staging
# ══════════════════════════════════════════════

def load_master_data(**context):
    """Load 01_master_data into staging.public.master_data."""
    batch_folder = context["ti"].xcom_pull(key="batch_folder")
    filepath = resolve_filepath(batch_folder, "01_master_data")
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)

    engine = create_engine(STAGING_CONN)
    start = time.time()
    df = smart_read(filepath)
    # Append new batch data (DWH accumulates historical data)
    df.to_sql("master_data", engine, schema="public",
              if_exists="append", index=False)
    elapsed = time.time() - start

    print(f"Loaded {len(df)} rows into staging.master_data from {os.path.basename(filepath)}")
    log_metric("ingestion", {
        "TableName": "master_data",
        "SourceFile": os.path.basename(filepath),
        "BatchFolder": batch_folder,
        "RowsLoaded": len(df),
        "LoadTimeSec": round(elapsed, 2),
        "FileSizeMB": round(file_size_mb, 2),
        "ThroughputRowsPerSec": round(len(df) / max(elapsed, 0.01), 0),
        "Status": "SUCCESS",
    })


def load_pharmacy_sales(**context):
    """Load 05_pharmacy_sales_data into staging.public.pharmacy_sales."""
    batch_folder = context["ti"].xcom_pull(key="batch_folder")
    filepath = resolve_filepath(batch_folder, "05_pharmacy_sales_data")
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)

    engine = create_engine(STAGING_CONN)
    start = time.time()
    df = smart_read(filepath)
    df.to_sql("pharmacy_sales", engine, schema="public",
              if_exists="append", index=False, chunksize=50000)
    elapsed = time.time() - start

    print(f"Loaded {len(df)} rows into staging.pharmacy_sales from {os.path.basename(filepath)}")
    log_metric("ingestion", {
        "TableName": "pharmacy_sales",
        "SourceFile": os.path.basename(filepath),
        "BatchFolder": batch_folder,
        "RowsLoaded": len(df),
        "LoadTimeSec": round(elapsed, 2),
        "FileSizeMB": round(file_size_mb, 2),
        "ThroughputRowsPerSec": round(len(df) / max(elapsed, 0.01), 0),
        "Status": "SUCCESS",
    })


def load_hospital_demand(**context):
    """Load 06_hospital_demand_data into staging.public.hospital_demand."""
    batch_folder = context["ti"].xcom_pull(key="batch_folder")
    filepath = resolve_filepath(batch_folder, "06_hospital_demand_data")
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)

    engine = create_engine(STAGING_CONN)
    start = time.time()
    df = smart_read(filepath)
    df.to_sql("hospital_demand", engine, schema="public",
              if_exists="append", index=False)
    elapsed = time.time() - start

    print(f"Loaded {len(df)} rows into staging.hospital_demand from {os.path.basename(filepath)}")
    log_metric("ingestion", {
        "TableName": "hospital_demand",
        "SourceFile": os.path.basename(filepath),
        "BatchFolder": batch_folder,
        "RowsLoaded": len(df),
        "LoadTimeSec": round(elapsed, 2),
        "FileSizeMB": round(file_size_mb, 2),
        "ThroughputRowsPerSec": round(len(df) / max(elapsed, 0.01), 0),
        "Status": "SUCCESS",
    })


# ══════════════════════════════════════════════
# Task: Streaming Ingestion (07_iot_streaming_data.csv)
# ══════════════════════════════════════════════

def ingest_iot_streaming(**context):
    """
    Ingest 07_iot_streaming_data.csv row-by-row (simulated streaming).
    Validates each row, counts malformed/dropped records, and logs
    streaming metrics for the Streaming Ingestion Dashboard.
    """
    batch_folder = context["ti"].xcom_pull(key="batch_folder")
    filepath = resolve_filepath(batch_folder, SDI_STREAM_FILE)
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)

    print(f"Starting streaming ingestion from {os.path.basename(filepath)}")
    start = time.time()

    df = pd.read_csv(filepath, low_memory=False)
    total_rows = len(df)

    # ── Validate rows (detect dirty data) ──
    # Null DeviceID = dropped (key violation)
    null_device = df["DeviceID"].isna().sum()

    # Non-numeric TemperatureC = dropped (schema mismatch)
    temp_numeric = pd.to_numeric(df["TemperatureC"], errors="coerce")
    bad_temp = temp_numeric.isna().sum() - df["TemperatureC"].isna().sum()  # only ERR_NAN, not original NaN

    dropped_records = int(null_device + bad_temp)
    valid_records = total_rows - dropped_records

    # ── Load valid rows into PostgreSQL staging ──
    engine = create_engine(STAGING_CONN)
    df_valid = df[df["DeviceID"].notna()].copy()
    df_valid["TemperatureC"] = pd.to_numeric(df_valid["TemperatureC"], errors="coerce")
    df_valid = df_valid[df_valid["TemperatureC"].notna()]

    # Ensure all flag columns are stored as text (Y/N), not numeric
    flag_cols = ["DoorOpenFlag", "TemperatureBreachFlag", "HumidityBreachFlag",
                 "ThresholdBreachFlag", "LateArrivalFlag"]
    for col in flag_cols:
        if col in df_valid.columns:
            df_valid[col] = df_valid[col].astype(str)

    # Drop and recreate table to avoid type mismatches from prior runs
    engine.execute("DROP TABLE IF EXISTS public.iot_streaming")

    df_valid.to_sql("iot_streaming", engine, schema="public",
                    if_exists="append", index=False, chunksize=10000)

    elapsed = time.time() - start

    # ── Compute streaming metrics ──
    throughput_eps = round(total_rows / max(elapsed, 0.01), 2)  # events per second

    # Average data latency from the DataLatencySeconds column
    latency_col = pd.to_numeric(df_valid["DataLatencySeconds"], errors="coerce")
    avg_latency = round(latency_col.mean(), 2) if not latency_col.isna().all() else 0

    # Late arrivals (Y/N flags)
    late_arrivals = int((df_valid["LateArrivalFlag"].str.upper() == "Y").sum()) if "LateArrivalFlag" in df_valid.columns else 0

    # Breach counts (Y/N flags)
    temp_breaches = int((df_valid["TemperatureBreachFlag"].str.upper() == "Y").sum()) if "TemperatureBreachFlag" in df_valid.columns else 0
    hum_breaches = int((df_valid["HumidityBreachFlag"].str.upper() == "Y").sum()) if "HumidityBreachFlag" in df_valid.columns else 0

    print(f"\n  Streaming Ingestion Results:")
    print(f"    Total records:    {total_rows:,}")
    print(f"    Valid records:    {valid_records:,}")
    print(f"    Dropped records:  {dropped_records:,} (null DeviceID: {null_device}, bad TemperatureC: {bad_temp})")
    print(f"    Throughput:       {throughput_eps:,.0f} events/sec")
    print(f"    Avg latency:      {avg_latency} sec")
    print(f"    Temp breaches:    {temp_breaches:,}")
    print(f"    Humidity breaches:{hum_breaches:,}")

    # ── Log streaming metrics ──
    log_metric("streaming", {
        "SourceFile": os.path.basename(filepath),
        "BatchFolder": batch_folder,
        "TotalRecords": total_rows,
        "ValidRecords": valid_records,
        "DroppedRecords": dropped_records,
        "NullKeyViolations": int(null_device),
        "SchemaMismatchErrors": int(bad_temp),
        "ThroughputEPS": throughput_eps,
        "AvgLatencySec": avg_latency,
        "LateArrivals": late_arrivals,
        "TempBreaches": temp_breaches,
        "HumidityBreaches": hum_breaches,
        "LoadTimeSec": round(elapsed, 2),
        "FileSizeMB": round(file_size_mb, 2),
        "Status": "SUCCESS",
    })


# ══════════════════════════════════════════════
# Task: Log storage metrics
# ══════════════════════════════════════════════

def log_ingestion_summary(**context):
    """Log storage metrics for PostgreSQL and DuckDB tables."""
    batch_folder = context["ti"].xcom_pull(key="batch_folder")
    engine = create_engine(STAGING_CONN)
    tables = ["master_data", "pharmacy_sales", "hospital_demand", "iot_streaming"]

    print("\n" + "=" * 60)
    print(f"BATCH INGESTION SUMMARY  (source: {batch_folder})")
    print("=" * 60)

    storage_rows = []
    for t in tables:
        try:
            count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM public.{t}", engine)
            row_count = count["cnt"][0]

            size_result = pd.read_sql(
                f"SELECT pg_total_relation_size('public.{t}') as size_bytes", engine
            )
            size_mb = round(size_result["size_bytes"][0] / (1024 * 1024), 2)

            print(f"  {t:25s} | {row_count:>10,} rows | {size_mb:>8.2f} MB")

            storage_rows.append({
                "Layer": "Staging (PostgreSQL)",
                "TableName": t,
                "RowCount": row_count,
                "SizeMB": size_mb,
                "BatchFolder": batch_folder,
            })
        except Exception as e:
            print(f"  {t:25s} | ERROR: {e}")

    # DuckDB OLAP table sizes (if available from prior runs)
    try:
        import duckdb
        duckdb_path = "/opt/airflow/duckdb/clarity_pharma_sdi.duckdb"
        if os.path.exists(duckdb_path):
            con = duckdb.connect(duckdb_path, read_only=True)
            olap_tables = con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()

            print(f"\n  {'DuckDB OLAP Tables':25s}")
            print(f"  {'-'*50}")
            for (tbl,) in olap_tables:
                try:
                    cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                    storage_rows.append({
                        "Layer": "OLAP (DuckDB)",
                        "TableName": tbl,
                        "RowCount": cnt,
                        "SizeMB": 0,
                        "BatchFolder": batch_folder,
                    })
                    print(f"  {tbl:25s} | {cnt:>10,} rows")
                except Exception:
                    pass
            con.close()

            duckdb_size_mb = round(os.path.getsize(duckdb_path) / (1024 * 1024), 2)
            print(f"\n  DuckDB file size: {duckdb_size_mb} MB")
    except Exception as e:
        print(f"  DuckDB metrics skipped: {e}")

    # Save storage metrics
    for row in storage_rows:
        log_metric("storage", row)

    print("=" * 60)


# ══════════════════════════════════════════════
# Task: Sync metrics CSVs into PostgreSQL (for Grafana)
# ══════════════════════════════════════════════

def sync_metrics_to_postgres(**context):
    """
    Reload all metrics CSVs into PostgreSQL so Grafana dashboards
    auto update after every pipeline run. Replaces the tables each time
    so there are no duplicates.
    """
    engine = create_engine(STAGING_CONN)
    metrics_files = {
        "ingestion_metrics": os.path.join(METRICS_DIR, "ingestion_metrics.csv"),
        "streaming_metrics": os.path.join(METRICS_DIR, "streaming_metrics.csv"),
        "storage_metrics": os.path.join(METRICS_DIR, "storage_metrics.csv"),
    }

    for table_name, csv_path in metrics_files.items():
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path, low_memory=False)
            df.to_sql(table_name, engine, schema="public",
                      if_exists="replace", index=False)
            print(f"Synced {table_name}: {len(df)} rows into PostgreSQL")
        else:
            print(f"Skipped {table_name}: CSV not found at {csv_path}")

    # Also sync query_performance_log if it exists (from dag_03)
    qp_path = os.path.join(METRICS_DIR, "query_performance_log.csv")
    if os.path.exists(qp_path):
        df = pd.read_csv(qp_path, low_memory=False)
        df.to_sql("query_performance_log", engine, schema="public",
                  if_exists="replace", index=False)
        print(f"Synced query_performance_log: {len(df)} rows into PostgreSQL")

    print("All metrics synced to PostgreSQL for Grafana.")


# ══════════════════════════════════════════════
# FileSensor: Wait for at least one SDI file in batch folder
# ══════════════════════════════════════════════

t_file_sensor = FileSensor(
    task_id="wait_for_source_files",
    filepath="/opt/airflow/data/{{ dag_run.conf.get('batch_folder', 'batch_1') }}/05_pharmacy_sales_data.*",
    fs_conn_id="fs_default",
    poke_interval=10,
    timeout=120,
    mode="poke",
    dag=dag,
)


# ══════════════════════════════════════════════
# Task Definitions
# ══════════════════════════════════════════════

t_check_files = PythonOperator(
    task_id="check_source_files",
    python_callable=check_source_files,
    provide_context=True,
    dag=dag,
)

t_load_master = PythonOperator(
    task_id="load_master_data",
    python_callable=load_master_data,
    provide_context=True,
    dag=dag,
)

t_load_sales = PythonOperator(
    task_id="load_pharmacy_sales",
    python_callable=load_pharmacy_sales,
    provide_context=True,
    dag=dag,
)

t_load_demand = PythonOperator(
    task_id="load_hospital_demand",
    python_callable=load_hospital_demand,
    provide_context=True,
    dag=dag,
)

t_load_iot = PythonOperator(
    task_id="ingest_iot_streaming",
    python_callable=ingest_iot_streaming,
    provide_context=True,
    dag=dag,
)

t_summary = PythonOperator(
    task_id="ingestion_summary",
    python_callable=log_ingestion_summary,
    provide_context=True,
    dag=dag,
)

t_sync_metrics = PythonOperator(
    task_id="sync_metrics_to_postgres",
    python_callable=sync_metrics_to_postgres,
    provide_context=True,
    dag=dag,
)

t_trigger_transform = TriggerDagRunOperator(
    task_id="trigger_dag_02_transform",
    trigger_dag_id="dag_02_transform_sdi",
    conf={"batch_folder": "{{ ti.xcom_pull(key='batch_folder') }}"},
    dag=dag,
)


# ══════════════════════════════════════════════
# Dependencies
# ══════════════════════════════════════════════
# FileSensor -> check -> [load tasks in parallel] -> summary -> sync_metrics -> trigger dag_02
#
#                          ┌──► load_master  ──┐
#  sensor → check_files ───┼──► load_sales  ───┼──► summary → sync_metrics → trigger_dag_02
#                          ├──► load_demand ───┤
#                          └──► load_iot    ───┘

t_file_sensor >> t_check_files
t_check_files >> [t_load_master, t_load_sales, t_load_demand, t_load_iot]
[t_load_master, t_load_sales, t_load_demand, t_load_iot] >> t_summary
t_summary >> t_sync_metrics >> t_trigger_transform