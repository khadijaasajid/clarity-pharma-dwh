"""
DAG 03 - Analytical Query Layer & Optimization
================================================
Implements OLAP indexing, pre-aggregated summary tables
(materialized views), and query performance logging.

This layer speeds up complex multi-fact queries and logs
execution times for the Query Performance Dashboard.

Summary Tables Created:
  - mv_sales_by_disease_quarter: Net sales by disease, channel, quarter
  - mv_hospital_demand_summary: Unfulfilled demand + stockouts by hospital
  - mv_margin_by_pharmacy_segment: Gross margin + returns by pharmacy type
  - mv_monthly_sales_trend: Monthly sales trend with YoY comparison
  - mv_medicine_performance: Medicine-level KPIs (sales, margin, returns)

Performance Log Table:
  - query_performance_log: Tracks execution time of each query
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import time
import os

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
DUCKDB_PATH = "/opt/airflow/duckdb/clarity_pharma_sdi.duckdb"

default_args = {
    "owner": "clarity_pharma",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="dag_03_query_layer",
    default_args=default_args,
    description="OLAP indexing, materialized views, and query performance logging",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sdi", "query_layer", "optimization"],
)


# ══════════════════════════════════════════════
# Helper: Run query with timing
# ══════════════════════════════════════════════

def timed_query(con, query_name, sql):
    """Execute a query and log execution time."""
    start = time.time()
    result = con.execute(sql)
    elapsed_ms = round((time.time() - start) * 1000, 2)
    print(f"  {query_name}: {elapsed_ms} ms")

    # Log to performance table
    con.execute("""
        INSERT INTO query_performance_log
        (QueryName, ExecutionTimeMs, ExecutedAt, Status, RowsReturned)
        VALUES (?, ?, NOW(), 'SUCCESS', ?)
    """, [query_name, elapsed_ms, result.fetchone()[0] if query_name.startswith("count_") else 0])

    return elapsed_ms


def timed_execute(con, query_name, sql):
    """Execute DDL/DML with timing and log it."""
    start = time.time()
    con.execute(sql)
    elapsed_ms = round((time.time() - start) * 1000, 2)
    print(f"  {query_name}: {elapsed_ms} ms")

    con.execute("""
        INSERT INTO query_performance_log
        (QueryName, ExecutionTimeMs, ExecutedAt, Status, RowsReturned)
        VALUES (?, ?, NOW(), 'SUCCESS', 0)
    """, [query_name, elapsed_ms])

    return elapsed_ms


# ══════════════════════════════════════════════
# Task 1: Create Performance Log Table + Indexes
# ══════════════════════════════════════════════

def create_indexes_and_log():
    """Create OLAP indexes and query performance log table."""
    con = duckdb.connect(DUCKDB_PATH)

    print("=" * 60)
    print("CREATING QUERY PERFORMANCE LOG TABLE")
    print("=" * 60)

    con.execute("DROP TABLE IF EXISTS query_performance_log")
    con.execute("""
        CREATE TABLE query_performance_log (
            LogID               INTEGER,
            QueryName           VARCHAR(100),
            ExecutionTimeMs     DOUBLE,
            ExecutedAt          TIMESTAMP,
            Status              VARCHAR(20),
            RowsReturned        INTEGER DEFAULT 0
        )
    """)

    print("  query_performance_log table created")

    print("\n" + "=" * 60)
    print("CREATING OLAP INDEXES")
    print("=" * 60)

    # DuckDB uses ART indexes. Create indexes on FK columns for faster joins
    indexes = [
        ("idx_fact_sales_datesk", "FactPharmacySales", "SaleDateSK"),
        ("idx_fact_sales_medsk", "FactPharmacySales", "MedicineSK"),
        ("idx_fact_sales_pharmsk", "FactPharmacySales", "PharmacySK"),
        ("idx_fact_sales_locsk", "FactPharmacySales", "LocationSK"),
        ("idx_fact_sales_profilesk", "FactPharmacySales", "SalesProfileSK"),
        ("idx_fact_demand_datesk", "FactHospitalDemand", "DemandMonthSK"),
        ("idx_fact_demand_medsk", "FactHospitalDemand", "MedicineSK"),
        ("idx_fact_demand_hospsk", "FactHospitalDemand", "HospitalSK"),
        ("idx_fact_demand_locsk", "FactHospitalDemand", "LocationSK"),
        ("idx_dim_date_year_qtr", "DimDate", "Year, Quarter"),
        ("idx_dim_medicine_disease", "DimMedicine", "DiseaseCategory"),
        ("idx_dim_medicine_therapeutic", "DimMedicine", "TherapeuticClass"),
    ]

    for idx_name, table, columns in indexes:
        try:
            con.execute(f"DROP INDEX IF EXISTS {idx_name}")
            con.execute(f"CREATE INDEX {idx_name} ON {table}({columns})")
            print(f"  Created: {idx_name} on {table}({columns})")
        except Exception as e:
            print(f"  Skipped {idx_name}: {e}")

    con.close()
    print("\nIndexes and log table ready.")


# ══════════════════════════════════════════════
# Task 2: Build Pre-Aggregated Summary Tables
# ══════════════════════════════════════════════

def build_summary_tables():
    """Create materialized views as pre-aggregated summary tables."""
    con = duckdb.connect(DUCKDB_PATH)

    print("=" * 60)
    print("BUILDING PRE-AGGREGATED SUMMARY TABLES")
    print("=" * 60)

    # ── MV1: Sales by Disease Category, Channel, Quarter ──
    print("\n  Building mv_sales_by_disease_quarter...")
    con.execute("DROP TABLE IF EXISTS mv_sales_by_disease_quarter")
    con.execute("""
        CREATE TABLE mv_sales_by_disease_quarter AS
        SELECT
            dm.DiseaseCategory,
            sp.SalesChannel,
            dd.Year,
            dd.Quarter,
            dd.MonthName,
            COUNT(*) AS TransactionCount,
            SUM(f.QuantitySoldPacks) AS TotalQuantitySold,
            ROUND(SUM(f.GrossSalesAmountPKR), 2) AS TotalGrossSalesPKR,
            ROUND(SUM(f.DiscountAmountPKR), 2) AS TotalDiscountPKR,
            ROUND(SUM(f.NetSalesAmountPKR), 2) AS TotalNetSalesPKR,
            ROUND(SUM(f.GrossMarginPKR), 2) AS TotalMarginPKR,
            ROUND(AVG(f.NetSalesAmountPKR), 2) AS AvgNetSalePerTxn,
            SUM(f.ReturnedQuantityPacks) AS TotalReturns
        FROM FactPharmacySales f
        JOIN DimMedicine dm ON f.MedicineSK = dm.MedicineSK
        JOIN DimSalesProfile sp ON f.SalesProfileSK = sp.SalesProfileSK
        JOIN DimDate dd ON f.SaleDateSK = dd.DateSK
        GROUP BY dm.DiseaseCategory, sp.SalesChannel,
                 dd.Year, dd.Quarter, dd.MonthName
    """)
    cnt = con.execute("SELECT COUNT(*) FROM mv_sales_by_disease_quarter").fetchone()[0]
    print(f"    Rows: {cnt:,}")

    # ── MV2: Hospital Demand Summary ──
    print("\n  Building mv_hospital_demand_summary...")
    con.execute("DROP TABLE IF EXISTS mv_hospital_demand_summary")
    con.execute("""
        CREATE TABLE mv_hospital_demand_summary AS
        SELECT
            dh.HospitalName,
            dh.HospitalType,
            dh.EmergencyLevel,
            dl.City,
            dl.State,
            dl.Region,
            dd.Year,
            dd.Quarter,
            dm.DiseaseCategory,
            SUM(f.ForecastDemandPacks) AS TotalForecast,
            SUM(f.QuantityReceivedPacks) AS TotalReceived,
            SUM(f.ActualConsumptionPacks) AS TotalConsumed,
            SUM(f.UnfulfilledDemandPacks) AS TotalUnfulfilled,
            SUM(f.StockoutDays) AS TotalStockoutDays,
            ROUND(AVG(f.ServiceLevelPct), 2) AS AvgServiceLevel,
            SUM(f.EmergencyOrderCount) AS TotalEmergencyOrders,
            SUM(f.QuantityExpiredPacks) AS TotalExpired,
            ROUND(SUM(f.WastageValuePKR), 2) AS TotalWastageValuePKR,
            ROUND(SUM(f.InventoryCarryingCostPKR), 2) AS TotalCarryingCostPKR,
            ROUND(AVG(f.DaysOfSupply), 1) AS AvgDaysOfSupply
        FROM FactHospitalDemand f
        JOIN DimHospital dh ON f.HospitalSK = dh.HospitalSK
        JOIN DimLocation dl ON f.LocationSK = dl.LocationSK
        JOIN DimDate dd ON f.DemandMonthSK = dd.DateSK
        JOIN DimMedicine dm ON f.MedicineSK = dm.MedicineSK
        GROUP BY dh.HospitalName, dh.HospitalType, dh.EmergencyLevel,
                 dl.City, dl.State, dl.Region,
                 dd.Year, dd.Quarter, dm.DiseaseCategory
    """)
    cnt = con.execute("SELECT COUNT(*) FROM mv_hospital_demand_summary").fetchone()[0]
    print(f"    Rows: {cnt:,}")

    # ── MV3: Margin by Pharmacy Type + Segment ──
    print("\n  Building mv_margin_by_pharmacy_segment...")
    con.execute("DROP TABLE IF EXISTS mv_margin_by_pharmacy_segment")
    con.execute("""
        CREATE TABLE mv_margin_by_pharmacy_segment AS
        SELECT
            dp.PharmacyType,
            sp.CustomerSegment,
            sp.SalesChannel,
            sp.PaymentMethod,
            dd.Year,
            dd.Quarter,
            COUNT(*) AS TransactionCount,
            SUM(f.QuantitySoldPacks) AS TotalSold,
            ROUND(SUM(f.NetSalesAmountPKR), 2) AS TotalNetSalesPKR,
            ROUND(SUM(f.COGSAmountPKR), 2) AS TotalCOGSPKR,
            ROUND(SUM(f.GrossMarginPKR), 2) AS TotalMarginPKR,
            ROUND(SUM(f.GrossMarginPKR) * 100.0
                  / NULLIF(SUM(f.NetSalesAmountPKR), 0), 2) AS MarginPct,
            SUM(f.ReturnedQuantityPacks) AS TotalReturns,
            ROUND(SUM(f.ReturnedQuantityPacks) * 100.0
                  / NULLIF(SUM(f.QuantitySoldPacks), 0), 2) AS ReturnRatePct
        FROM FactPharmacySales f
        JOIN DimPharmacy dp ON f.PharmacySK = dp.PharmacySK
        JOIN DimSalesProfile sp ON f.SalesProfileSK = sp.SalesProfileSK
        JOIN DimDate dd ON f.SaleDateSK = dd.DateSK
        GROUP BY dp.PharmacyType, sp.CustomerSegment,
                 sp.SalesChannel, sp.PaymentMethod,
                 dd.Year, dd.Quarter
    """)
    cnt = con.execute("SELECT COUNT(*) FROM mv_margin_by_pharmacy_segment").fetchone()[0]
    print(f"    Rows: {cnt:,}")

    # ── MV4: Monthly Sales Trend ──
    print("\n  Building mv_monthly_sales_trend...")
    con.execute("DROP TABLE IF EXISTS mv_monthly_sales_trend")
    con.execute("""
        CREATE TABLE mv_monthly_sales_trend AS
        SELECT
            dd.Year,
            dd.MonthNumber,
            dd.MonthName,
            COUNT(*) AS TransactionCount,
            SUM(f.QuantitySoldPacks) AS TotalQuantitySold,
            ROUND(SUM(f.NetSalesAmountPKR), 2) AS TotalNetSalesPKR,
            ROUND(SUM(f.GrossMarginPKR), 2) AS TotalMarginPKR,
            ROUND(AVG(f.NetSalesAmountPKR), 2) AS AvgSaleAmountPKR,
            SUM(f.ReturnedQuantityPacks) AS TotalReturns,
            COUNT(DISTINCT f.PharmacySK) AS ActivePharmacies,
            COUNT(DISTINCT f.MedicineSK) AS UniqueMedicines
        FROM FactPharmacySales f
        JOIN DimDate dd ON f.SaleDateSK = dd.DateSK
        GROUP BY dd.Year, dd.MonthNumber, dd.MonthName
        ORDER BY dd.Year, dd.MonthNumber
    """)
    cnt = con.execute("SELECT COUNT(*) FROM mv_monthly_sales_trend").fetchone()[0]
    print(f"    Rows: {cnt:,}")

    # ── MV5: Medicine Performance ──
    print("\n  Building mv_medicine_performance...")
    con.execute("DROP TABLE IF EXISTS mv_medicine_performance")
    con.execute("""
        CREATE TABLE mv_medicine_performance AS
        SELECT
            dm.MedicineID,
            dm.MedicineName,
            dm.BrandName,
            dm.TherapeuticClass,
            dm.DiseaseCategory,
            dm.DrugSchedule,
            COUNT(*) AS TotalTransactions,
            SUM(f.QuantitySoldPacks) AS TotalQuantitySold,
            ROUND(SUM(f.GrossSalesAmountPKR), 2) AS TotalGrossSalesPKR,
            ROUND(SUM(f.NetSalesAmountPKR), 2) AS TotalNetSalesPKR,
            ROUND(SUM(f.GrossMarginPKR), 2) AS TotalMarginPKR,
            ROUND(SUM(f.GrossMarginPKR) * 100.0
                  / NULLIF(SUM(f.NetSalesAmountPKR), 0), 2) AS MarginPct,
            SUM(f.ReturnedQuantityPacks) AS TotalReturns,
            ROUND(AVG(f.DaysToExpiryAtSale), 1) AS AvgDaysToExpiry,
            COUNT(DISTINCT f.PharmacySK) AS PharmaciesServed
        FROM FactPharmacySales f
        JOIN DimMedicine dm ON f.MedicineSK = dm.MedicineSK
        GROUP BY dm.MedicineID, dm.MedicineName, dm.BrandName,
                 dm.TherapeuticClass, dm.DiseaseCategory, dm.DrugSchedule
    """)
    cnt = con.execute("SELECT COUNT(*) FROM mv_medicine_performance").fetchone()[0]
    print(f"    Rows: {cnt:,}")

    con.close()
    print("\nAll summary tables built.")


# ══════════════════════════════════════════════
# Task 3: Run Analytical Queries with Logging
# ══════════════════════════════════════════════

def run_analytical_queries():
    """Run analytical queries and log performance."""
    con = duckdb.connect(DUCKDB_PATH)

    print("=" * 60)
    print("RUNNING ANALYTICAL QUERIES WITH PERFORMANCE LOGGING")
    print("=" * 60)

    queries = {
        "Q1_Sales_By_Disease_Channel_Quarter": """
            SELECT DiseaseCategory, SalesChannel, Year, Quarter,
                   TotalNetSalesPKR, TransactionCount
            FROM mv_sales_by_disease_quarter
            ORDER BY TotalNetSalesPKR DESC
            LIMIT 20
        """,
        "Q2_Hospital_Unfulfilled_Demand": """
            SELECT HospitalName, HospitalType, City,
                   TotalUnfulfilled, TotalStockoutDays, AvgServiceLevel
            FROM mv_hospital_demand_summary
            WHERE Year = (SELECT MAX(Year) FROM mv_hospital_demand_summary)
            ORDER BY TotalUnfulfilled DESC
            LIMIT 15
        """,
        "Q3_Margin_Return_By_PharmType": """
            SELECT PharmacyType, CustomerSegment,
                   TotalNetSalesPKR, TotalMarginPKR, MarginPct,
                   TotalReturns, ReturnRatePct
            FROM mv_margin_by_pharmacy_segment
            ORDER BY MarginPct DESC
            LIMIT 15
        """,
        "Q4_Monthly_Sales_Trend": """
            SELECT Year, MonthNumber, MonthName,
                   TotalNetSalesPKR, TotalQuantitySold,
                   TotalMarginPKR, ActivePharmacies
            FROM mv_monthly_sales_trend
            ORDER BY Year, MonthNumber
        """,
        "Q5_Top_Medicines_By_Revenue": """
            SELECT MedicineName, TherapeuticClass,
                   TotalNetSalesPKR, TotalQuantitySold,
                   MarginPct, TotalReturns, PharmaciesServed
            FROM mv_medicine_performance
            ORDER BY TotalNetSalesPKR DESC
            LIMIT 15
        """,
        "Q6_Disease_Demand_Vs_Sales": """
            SELECT
                s.DiseaseCategory,
                SUM(s.TotalNetSalesPKR) AS SalesRevenuePKR,
                SUM(s.TotalQuantitySold) AS SalesQuantity,
                d.TotalConsumed AS HospitalConsumed,
                d.TotalUnfulfilled AS HospitalUnfulfilled
            FROM mv_sales_by_disease_quarter s
            LEFT JOIN (
                SELECT DiseaseCategory,
                       SUM(TotalConsumed) AS TotalConsumed,
                       SUM(TotalUnfulfilled) AS TotalUnfulfilled
                FROM mv_hospital_demand_summary
                GROUP BY DiseaseCategory
            ) d ON s.DiseaseCategory = d.DiseaseCategory
            GROUP BY s.DiseaseCategory, d.TotalConsumed, d.TotalUnfulfilled
            ORDER BY SalesRevenuePKR DESC
        """,
        "Q7_Stockout_Risk_Hospitals": """
            SELECT HospitalName, City, State,
                   TotalStockoutDays, AvgServiceLevel,
                   TotalEmergencyOrders, TotalExpired
            FROM mv_hospital_demand_summary
            WHERE TotalStockoutDays > 0
            ORDER BY TotalStockoutDays DESC
            LIMIT 15
        """,
        "Q8_Channel_Revenue_Split": """
            SELECT SalesChannel, Year,
                   SUM(TotalNetSalesPKR) AS RevenuePKR,
                   SUM(TransactionCount) AS Transactions,
                   ROUND(SUM(TotalNetSalesPKR) * 100.0
                         / SUM(SUM(TotalNetSalesPKR)) OVER (PARTITION BY Year), 2)
                         AS RevenuePctOfYear
            FROM mv_sales_by_disease_quarter
            GROUP BY SalesChannel, Year
            ORDER BY Year, RevenuePKR DESC
        """,
        # ── Intentional failure queries to validate error monitoring ──
        "Q9_FAIL_Missing_Column": """
            SELECT NonExistentColumn, MedicineName
            FROM DimMedicine
            LIMIT 10
        """,
        "Q10_FAIL_Missing_Table": """
            SELECT * FROM inventory_forecast_2027
            LIMIT 5
        """,
    }

    # Get current max LogID
    try:
        max_id = con.execute("SELECT COALESCE(MAX(LogID), 0) FROM query_performance_log").fetchone()[0]
    except Exception:
        max_id = 0
    log_id = max_id

    total_time = 0
    query_count = 0
    error_count = 0

    for query_name, sql in queries.items():
        try:
            start = time.time()
            result = con.execute(sql).df()
            elapsed_ms = round((time.time() - start) * 1000, 2)
            rows = len(result)
            total_time += elapsed_ms
            query_count += 1
            log_id += 1

            con.execute("""
                INSERT INTO query_performance_log
                (LogID, QueryName, ExecutionTimeMs, ExecutedAt, Status, RowsReturned)
                VALUES (?, ?, ?, NOW(), 'SUCCESS', ?)
            """, [log_id, query_name, elapsed_ms, rows])

            print(f"\n  {query_name}: {elapsed_ms} ms ({rows} rows)")
            print(result.head(5).to_string(index=False))

        except Exception as e:
            elapsed_ms = round((time.time() - start) * 1000, 2)
            error_count += 1
            log_id += 1

            con.execute("""
                INSERT INTO query_performance_log
                (LogID, QueryName, ExecutionTimeMs, ExecutedAt, Status, RowsReturned)
                VALUES (?, ?, ?, NOW(), 'FAILED', 0)
            """, [log_id, query_name, elapsed_ms])

            print(f"\n  {query_name}: FAILED ({e})")

    # Summary
    print("\n" + "=" * 60)
    print("QUERY PERFORMANCE SUMMARY")
    print("=" * 60)
    print(f"  Total queries: {query_count}")
    print(f"  Failed queries: {error_count}")
    print(f"  Total execution time: {total_time:.2f} ms")
    print(f"  Average per query: {total_time / max(query_count, 1):.2f} ms")
    success_rate = round((query_count - error_count) / max(query_count, 1) * 100, 1)
    print(f"  Success rate: {success_rate}%")

    # Show full performance log
    print("\n" + "-" * 60)
    print("FULL PERFORMANCE LOG")
    print("-" * 60)
    log = con.execute("""
        SELECT QueryName, ExecutionTimeMs, Status, RowsReturned, ExecutedAt
        FROM query_performance_log
        ORDER BY LogID DESC
        LIMIT 20
    """).df()
    print(log.to_string(index=False))

    con.close()


# ══════════════════════════════════════════════
# Task 4: Export Summary Tables to CSV
# ══════════════════════════════════════════════

def export_summary_csvs():
    """Export summary tables and performance log to CSV for Power BI."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    export_dir = "/opt/airflow/data/exports"
    os.makedirs(export_dir, exist_ok=True)

    tables = [
        "mv_sales_by_disease_quarter",
        "mv_hospital_demand_summary",
        "mv_margin_by_pharmacy_segment",
        "mv_monthly_sales_trend",
        "mv_medicine_performance",
        "query_performance_log",
    ]

    # Also export the main star schema tables
    star_tables = [
        "DimDate", "DimMedicine", "DimPharmacy", "DimHospital",
        "DimLocation", "DimSalesProfile",
        "FactPharmacySales", "FactHospitalDemand",
    ]

    all_tables = star_tables + tables

    print("=" * 60)
    print("EXPORTING TO CSV FOR POWER BI")
    print("=" * 60)

    for table in all_tables:
        try:
            filepath = os.path.join(export_dir, f"{table}.csv")
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            con.execute(f"COPY {table} TO '{filepath}' (HEADER, DELIMITER ',')")
            print(f"  {table}: {count:,} rows -> {table}.csv")
        except Exception as e:
            print(f"  {table}: SKIPPED ({e})")

    con.close()
    print(f"\nAll CSVs exported to {export_dir}/")
    print("Connect Power BI to these CSV files for dashboards.")


# ══════════════════════════════════════════════
# Task Definitions
# ══════════════════════════════════════════════

t_indexes = PythonOperator(
    task_id="create_indexes_and_log",
    python_callable=create_indexes_and_log,
    dag=dag,
)

t_summaries = PythonOperator(
    task_id="build_summary_tables",
    python_callable=build_summary_tables,
    dag=dag,
)

t_queries = PythonOperator(
    task_id="run_analytical_queries",
    python_callable=run_analytical_queries,
    dag=dag,
)

t_export = PythonOperator(
    task_id="export_csvs",
    python_callable=export_summary_csvs,
    dag=dag,
)


# ══════════════════════════════════════════════
# Dependencies (sequential for DuckDB single-writer)
# ══════════════════════════════════════════════
#
#  indexes → summary_tables → queries → export_csvs

t_indexes >> t_summaries >> t_queries >> t_export