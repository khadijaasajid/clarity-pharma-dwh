# Clarity Pharma Data Warehouse

Pharmaceutical supply chain data warehouse built with Apache Airflow, PostgreSQL, DuckDB, and Grafana. This project implements a Kimball star schema for the Sales and Demand Intelligence (SDI) data mart.

## Group Members

| Name | Student ID |
|------|-----------|
|  Khadija Sajid    | 36229      |
|  Syeda Rumaisa Javed    | 36227     |

## Project Structure

```
clarity-pharma-dwh/          # All pipeline code
  dags/                       # Airflow DAG definitions
    dag_00_data_generator.py  # Generates oversampled synthetic data
    dag_01_batch_ingestion.py # Loads CSVs/XLSX into PostgreSQL staging
    dag_02_transform_sdi.py   # Transforms staging to DuckDB star schema
    dag_03_query_layer.py     # Runs analytical queries and exports results
  scripts/
    generate_data.py          # Data generation logic (oversampling)
    pipeline_controller_api.py # FastAPI automation controller
  data/
    baseline/                 # Baseline data (2024 to 2025)
  grafana/
    provisioning/             # Auto-loaded datasources and dashboard configs
    dashboards/               # Grafana dashboard JSON files
  sql/                        # SQL scripts for staging table creation
  docker-compose.yml          # Full stack: Airflow, PostgreSQL, Grafana, API
  Dockerfile                  # Custom Airflow image with dependencies
  requirements.txt            # Python dependencies
report/
  Phase.1.Report.pdf         # Phase 1 project report without BI
excel/
  SDI.Excel.xlsx              # SDI data mart Excel file
video/
  demo_video.mp4              # 2 min demo video
```

## Tech Stack

| Component       | Technology        | Purpose                        |
|-----------------|-------------------|--------------------------------|
| Orchestration   | Apache Airflow 2.8| ETL pipeline scheduling        |
| Staging DB      | PostgreSQL 15     | ODS / staging layer            |
| OLAP DB         | DuckDB            | Star schema (fact constellation)|
| Monitoring      | Grafana 10.2      | DE dashboards (3 dashboards)   |
| API Controller  | FastAPI            | Pipeline automation            |
| Containerization| Docker Compose     | Full stack deployment          |

## Prerequisites

- Docker Desktop installed and running
- At least 8 GB RAM allocated to Docker
- Ports 8080, 8000, 3000, 5433, 5434 available

## How to Run

### 1. Clone the Repository

```bash
git clone https://github.com/rosettajoy/clarity-pharma-dwh.git
cd clarity-pharma-dwh
```

### 2. Start All Services

```bash
cd clarity-pharma-dwh
docker compose up -d --build
```

Wait about 2 minutes for all services to initialize.

### 3. Verify Services Are Running

| Service              | URL                          | Credentials   |
|----------------------|------------------------------|---------------|
| Airflow Webserver    | http://localhost:8080         | admin / admin |
| Pipeline Controller  | http://localhost:8000/docs    | (none)        |
| Grafana Dashboards   | http://localhost:3000         | admin / admin |

### 4. Run the Full Pipeline

Open the Pipeline Controller Swagger UI at http://localhost:8000/docs and hit the **POST /pipeline/reset-and-automate** endpoint. This will:

1. Wipe all old staging tables, metrics, and DuckDB
2. Load baseline data immediately (2023 to 2025)
3. Generate and load batch_1 after 3 minutes (H1 2026: Jan to Jun)
4. Generate and load batch_2 after 5 minutes (H2 2026: Jul to Dec)

Each wave follows the full DAG chain:

```
dag_00 (generate) -> dag_01 (ingest) -> dag_02 (transform) -> dag_03 (query)
```

### 5. Monitor Progress

- **Airflow UI** (localhost:8080): Watch DAG runs in real time
- **Pipeline Status**: GET http://localhost:8000/pipeline/status
- **Grafana** (localhost:3000): See dashboards update as data flows in

### 6. Stop Everything

```bash
docker compose down
```

To also remove stored data (volumes):

```bash
docker compose down -v
```

## Pipeline Architecture

```
Baseline Data (CSV/XLSX)
        |
  dag_00: Data Generator (oversampling for batch_1 and batch_2)
        |
  dag_01: Batch Ingestion (FileSensor -> PostgreSQL staging)
        |
  dag_02: Transform SDI (PostgreSQL -> DuckDB star schema)
        |
  dag_03: Query Layer (analytical queries + CSV exports)
```

## SDI Star Schema

- **FactPharmacySales**: pharmacy transaction facts
- **FactHospitalDemand**: hospital demand and fulfillment facts
- **DimMedicine**: medicine dimension (SCD Type 2 for price changes)
- **DimPharmacy**: pharmacy dimension
- **DimHospital**: hospital dimension
- **DimDate**: date dimension (role playing)
- **DimTransactionType**: junk dimension

## Grafana Dashboards

1. **Batch Ingestion**: Tracks batches processed, row counts, throughput, load times
2. **Storage and Staging**: Shows table sizes, row counts, date coverage in PostgreSQL
3. **Query Performance**: Monitors query execution times, success rates, row counts

## Notes

- All currency values are in PKR (Pakistani Rupees) at 280 PKR per USD
- Baseline data is not included in the repo due to size limits. It is generated when the pipeline runs.
- Date format in source CSVs is dd/mm/yyyy
- SCD Type 2 is applied on DimMedicine for price changes on MED010 and MED025 in batch_2