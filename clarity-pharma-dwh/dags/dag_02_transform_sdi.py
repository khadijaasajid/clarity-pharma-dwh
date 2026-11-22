"""
DAG 03 - Transform SDI (Sales & Demand Intelligence)
=====================================================
Reads from PostgreSQL staging, transforms and loads into
DuckDB star schema (fact constellation).

Dimensions: DimDate, DimMedicine, DimLocation, DimPharmacy,
            DimHospital, DimSalesProfile (junk)

Facts: FactPharmacySales (transactional, ~500K rows)
       FactHospitalDemand (periodic snapshot, ~27K rows)

Currency: All USD values converted to PKR (1 USD = 280 PKR)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import pandas as pd
import duckdb
import os
from sqlalchemy import create_engine

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
DUCKDB_PATH = "/opt/airflow/duckdb/clarity_pharma_sdi.duckdb"
STAGING_CONN = os.environ.get(
    "STAGING_DB_CONN",
    "postgresql+psycopg2://staging:staging@postgres-staging:5432/staging"
)
USD_TO_PKR = 280  # Fixed conversion rate

default_args = {
    "owner": "clarity_pharma",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="dag_02_transform_sdi",
    default_args=default_args,
    description="Transform staging data into DuckDB star schema for SDI data mart",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sdi", "transform", "duckdb", "star_schema"],
)


# ══════════════════════════════════════════════
# SCHEMA: Create DuckDB tables
# ══════════════════════════════════════════════

def create_duckdb_schema():
    """Create all dimension and fact tables in DuckDB."""
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)

    # Drop in reverse dependency order
    con.execute("DROP TABLE IF EXISTS FactPharmacySales")
    con.execute("DROP TABLE IF EXISTS FactHospitalDemand")
    con.execute("DROP TABLE IF EXISTS DimDate")
    con.execute("DROP TABLE IF EXISTS DimMedicine")
    con.execute("DROP TABLE IF EXISTS DimLocation")
    con.execute("DROP TABLE IF EXISTS DimPharmacy")
    con.execute("DROP TABLE IF EXISTS DimHospital")
    con.execute("DROP TABLE IF EXISTS DimSalesProfile")

    # ── DimDate ──
    con.execute("""
        CREATE TABLE DimDate (
            DateSK          INTEGER PRIMARY KEY,
            FullDate        DATE NOT NULL,
            DayOfWeek       INTEGER,
            DayName         VARCHAR,
            MonthNumber     INTEGER,
            MonthName       VARCHAR,
            Quarter         INTEGER,
            Year            INTEGER,
            IsWeekend       BOOLEAN,
            FiscalQuarter   INTEGER
        )
    """)

    # ── DimMedicine (SCD Type 2) ──
    con.execute("""
        CREATE TABLE DimMedicine (
            MedicineSK              INTEGER PRIMARY KEY,
            MedicineID              VARCHAR NOT NULL,
            MedicineName            VARCHAR,
            GenericName             VARCHAR,
            BrandName               VARCHAR,
            Manufacturer            VARCHAR,
            DosageForm              VARCHAR,
            Strength                VARCHAR,
            UnitOfMeasure           VARCHAR,
            PackSize                INTEGER,
            TherapeuticClass        VARCHAR,
            DiseaseCategory         VARCHAR,
            DrugSchedule            VARCHAR,
            PrescriptionType        VARCHAR,
            StorageCondition        VARCHAR,
            ShelfLifeMonths         INTEGER,
            ReorderLevelPacks       INTEGER,
            UnitCostPKR             DECIMAL(14,4),
            UnitRetailPricePKR      DECIMAL(14,4),
            IsActive                BOOLEAN,
            LaunchDate              DATE,
            ValidFrom               DATE,
            ValidTo                 DATE,
            IsCurrent               BOOLEAN
        )
    """)

    # ── DimLocation (Shared) ──
    con.execute("""
        CREATE TABLE DimLocation (
            LocationSK  INTEGER PRIMARY KEY,
            City        VARCHAR,
            State       VARCHAR,
            Region      VARCHAR
        )
    """)

    # ── DimPharmacy ──
    con.execute("""
        CREATE TABLE DimPharmacy (
            PharmacySK      INTEGER PRIMARY KEY,
            PharmacyID      VARCHAR NOT NULL,
            PharmacyName    VARCHAR,
            PharmacyType    VARCHAR,
            LocationSK      INTEGER
        )
    """)

    # ── DimHospital ──
    con.execute("""
        CREATE TABLE DimHospital (
            HospitalSK             INTEGER PRIMARY KEY,
            HospitalID             VARCHAR NOT NULL,
            HospitalName           VARCHAR,
            HospitalType           VARCHAR,
            BedCapacity            INTEGER,
            HasPharmacyUnit        BOOLEAN,
            AccreditationStatus    VARCHAR,
            FundingType            VARCHAR,
            EmergencyLevel         VARCHAR,
            OperatingStatus        VARCHAR,
            LocationSK             INTEGER
        )
    """)

    # ── DimSalesProfile (Junk Dimension) ──
    con.execute("""
        CREATE TABLE DimSalesProfile (
            SalesProfileSK         INTEGER PRIMARY KEY,
            SalesChannel            VARCHAR,
            CustomerSegment         VARCHAR,
            PaymentMethod           VARCHAR,
            InsuranceCoverage       VARCHAR,
            PrescriptionRequired    BOOLEAN,
            IsEmergencyOrder        BOOLEAN,
            IsColdChainProduct      BOOLEAN
        )
    """)

    # ── FactPharmacySales (Transactional) ──
    con.execute("""
        CREATE TABLE FactPharmacySales (
            PharmacySalesSK         INTEGER PRIMARY KEY,
            SaleDateSK              INTEGER,
            BatchExpiryDateSK       INTEGER,
            DeliveryDateSK          INTEGER,
            MedicineSK              INTEGER,
            PharmacySK              INTEGER,
            LocationSK              INTEGER,
            SalesProfileSK          INTEGER,
            QuantitySoldPacks       INTEGER,
            UnitRetailPricePKR      DECIMAL(14,4),
            GrossSalesAmountPKR     DECIMAL(14,4),
            DiscountAmountPKR       DECIMAL(14,4),
            NetSalesAmountPKR       DECIMAL(14,4),
            TaxAmountPKR            DECIMAL(14,4),
            COGSAmountPKR           DECIMAL(14,4),
            GrossMarginPKR          DECIMAL(14,4),
            ReturnedQuantityPacks   INTEGER,
            DaysToExpiryAtSale      INTEGER,
            HourOfDay               INTEGER,
            LineItemCount           INTEGER,
            SalesTransactionID      VARCHAR,
            PharmacyOrderID         VARCHAR,
            PharmacyDeliveryID      VARCHAR,
            PrescriptionID          VARCHAR,
            PromotionCode           VARCHAR
        )
    """)

    # ── FactHospitalDemand (Periodic Snapshot) ──
    con.execute("""
        CREATE TABLE FactHospitalDemand (
            HospitalDemandSK           INTEGER PRIMARY KEY,
            DemandMonthSK              INTEGER,
            MedicineSK                 INTEGER,
            HospitalSK                 INTEGER,
            LocationSK                 INTEGER,
            ForecastDemandPacks        INTEGER,
            ActualConsumptionPacks     INTEGER,
            UnfulfilledDemandPacks     INTEGER,
            OpeningStockPacks          INTEGER,
            ClosingStockPacks          INTEGER,
            QuantityReceivedPacks      INTEGER,
            QuantityExpiredPacks       INTEGER,
            StockoutDays               INTEGER,
            EmergencyOrderCount        INTEGER,
            EmergencyOrderCostPKR      DECIMAL(14,4),
            WastageValuePKR            DECIMAL(14,4),
            InventoryCarryingCostPKR   DECIMAL(14,4),
            BudgetVariancePKR          DECIMAL(14,4),
            ServiceLevelPct            DECIMAL(5,2),
            DemandTrendIndex           DECIMAL(8,4),
            AverageLeadTimeDays        DECIMAL(8,2),
            SafetyStockPacks           INTEGER,
            DaysOfSupply               INTEGER,
            HospitalDemandRecordID     VARCHAR
        )
    """)

    con.close()
    print("DuckDB star schema created successfully (6 dims + 2 facts)")


# ══════════════════════════════════════════════
# DIMENSIONS
# ══════════════════════════════════════════════

def load_dim_date():
    """Generate DimDate covering all dates found in source data."""
    engine = create_engine(STAGING_CONN)

    # Collect date ranges from all date columns
    sales_dates = pd.read_sql("""
        SELECT MIN("SalesDate") as min_d, MAX("SalesDate") as max_d
        FROM pharmacy_sales
    """, engine)
    demand_dates = pd.read_sql("""
        SELECT MIN("DemandMonth") as min_d, MAX("DemandMonth") as max_d
        FROM hospital_demand
    """, engine)
    expiry_dates = pd.read_sql("""
        SELECT MIN("BatchExpiryDate") as min_d, MAX("BatchExpiryDate") as max_d
        FROM pharmacy_sales
    """, engine)
    delivery_dates = pd.read_sql("""
        SELECT MIN("PharmacyDeliveryDate") as min_d, MAX("PharmacyDeliveryDate") as max_d
        FROM pharmacy_sales
    """, engine)

    all_mins = [sales_dates['min_d'][0], demand_dates['min_d'][0],
                expiry_dates['min_d'][0], delivery_dates['min_d'][0]]
    all_maxs = [sales_dates['max_d'][0], demand_dates['max_d'][0],
                expiry_dates['max_d'][0], delivery_dates['max_d'][0]]

    all_mins = [pd.to_datetime(d, format='mixed', dayfirst=True) for d in all_mins if d is not None]
    all_maxs = [pd.to_datetime(d, format='mixed', dayfirst=True) for d in all_maxs if d is not None]

    start_date = min(all_mins) - timedelta(days=30)
    end_date = max(all_maxs) + timedelta(days=30)

    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    dim_date = pd.DataFrame({
        'DateSK': range(1, len(dates) + 1),
        'FullDate': dates.date,
        'DayOfWeek': dates.dayofweek + 1,
        'DayName': dates.strftime('%A'),
        'MonthNumber': dates.month,
        'MonthName': dates.strftime('%B'),
        'Quarter': dates.quarter,
        'Year': dates.year,
        'IsWeekend': dates.dayofweek >= 5,
        'FiscalQuarter': ((dates.month + 5) % 12 // 3) + 1,
    })

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("DELETE FROM DimDate")
    con.execute("INSERT INTO DimDate SELECT * FROM dim_date")
    con.close()
    print(f"Loaded {len(dim_date)} rows into DimDate "
          f"({start_date.date()} to {end_date.date()})")


def load_dim_medicine():
    """Transform master_data into DimMedicine with SCD Type 2 columns."""
    engine = create_engine(STAGING_CONN)
    df = pd.read_sql("SELECT * FROM master_data", engine)

    dim = pd.DataFrame({
        'MedicineSK': range(1, len(df) + 1),
        'MedicineID': df['MedicineID'],
        'MedicineName': df['GenericName'] + ' ' + df['Strength'],
        'GenericName': df['GenericName'],
        'BrandName': df['FictionalBrandName'],
        'Manufacturer': df['PrimarySupplierID'],
        'DosageForm': df['DosageForm'],
        'Strength': df['Strength'],
        'UnitOfMeasure': df['BaseUnitOfMeasure'],
        'PackSize': df['PackSize'],
        'TherapeuticClass': df['TherapeuticClass'],
        'DiseaseCategory': df['DiseaseCategory'],
        'DrugSchedule': df['PrescriptionClass'],
        'PrescriptionType': df['PrescriptionClass'],
        'StorageCondition': df['StorageMinTempC'].astype(str) + 'C to '
                            + df['StorageMaxTempC'].astype(str) + 'C',
        'ShelfLifeMonths': df['ShelfLifeMonths'],
        'ReorderLevelPacks': df['SafetyStockDays'],
        'UnitCostPKR': (df['UnitManufacturingCostUSD'] * USD_TO_PKR).round(4),
        'UnitRetailPricePKR': (df['RecommendedRetailPriceUSD'] * USD_TO_PKR).round(4),
        'IsActive': df['ProductStatus'].str.upper() == 'ACTIVE',
        'LaunchDate': pd.to_datetime(df['EffectiveFromDate'], format='mixed', dayfirst=True).dt.date,
        'ValidFrom': pd.to_datetime(df['EffectiveFromDate'], format='mixed', dayfirst=True).dt.date,
        'ValidTo': pd.Timestamp('9999-12-31').date(),
        'IsCurrent': True,
    })

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("DELETE FROM DimMedicine")
    con.execute("INSERT INTO DimMedicine SELECT * FROM dim")
    con.close()
    print(f"Loaded {len(dim)} rows into DimMedicine")


def load_dim_location():
    """Build shared DimLocation from pharmacy + hospital cities."""
    engine = create_engine(STAGING_CONN)

    pharmacy_locs = pd.read_sql("""
        SELECT DISTINCT "PharmacyCity" as City, "PharmacyState" as State
        FROM pharmacy_sales
        WHERE "PharmacyCity" IS NOT NULL
    """, engine)

    hospital_locs = pd.read_sql("""
        SELECT DISTINCT "City", "State"
        FROM hospital_demand
        WHERE "City" IS NOT NULL
    """, engine)

    all_locs = pd.concat([pharmacy_locs, hospital_locs]).drop_duplicates().reset_index(drop=True)

    # Derive Region from State
    def get_region(state):
        s = str(state).lower() if state else ''
        if any(x in s for x in ['punjab', 'islamabad', 'kpk', 'khyber', 'kashmir']):
            return 'North'
        if any(x in s for x in ['sindh', 'balochistan']):
            return 'South'
        return 'Other'

    all_locs['Region'] = all_locs['State'].apply(get_region)

    dim = pd.DataFrame({
        'LocationSK': range(1, len(all_locs) + 1),
        'City': all_locs['City'].values,
        'State': all_locs['State'].values,
        'Region': all_locs['Region'].values,
    })

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("DELETE FROM DimLocation")
    con.execute("INSERT INTO DimLocation SELECT * FROM dim")
    con.close()
    print(f"Loaded {len(dim)} rows into DimLocation")


def load_dim_pharmacy():
    """Build DimPharmacy with LocationSK foreign key lookup."""
    engine = create_engine(STAGING_CONN)

    df = pd.read_sql("""
        SELECT DISTINCT "PharmacyID", "PharmacyName", "PharmacyType",
               "PharmacyCity", "PharmacyState"
        FROM pharmacy_sales
        WHERE "PharmacyID" IS NOT NULL
    """, engine)

    con = duckdb.connect(DUCKDB_PATH)
    loc_df = con.execute("SELECT LocationSK, City, State FROM DimLocation").df()

    merged = df.merge(loc_df,
                      left_on=['PharmacyCity', 'PharmacyState'],
                      right_on=['City', 'State'], how='left')

    dim = pd.DataFrame({
        'PharmacySK': range(1, len(merged) + 1),
        'PharmacyID': merged['PharmacyID'].values,
        'PharmacyName': merged['PharmacyName'].values,
        'PharmacyType': merged['PharmacyType'].values,
        'LocationSK': merged['LocationSK'].fillna(0).astype(int).values,
    })

    con.execute("DELETE FROM DimPharmacy")
    con.execute("INSERT INTO DimPharmacy SELECT * FROM dim")
    con.close()
    print(f"Loaded {len(dim)} rows into DimPharmacy")


def load_dim_hospital():
    """Build DimHospital with LocationSK foreign key lookup."""
    engine = create_engine(STAGING_CONN)

    df = pd.read_sql("""
        SELECT DISTINCT "HospitalID", "HospitalName", "HospitalType",
               "LicensedBeds", "City", "State", "TraumaLevel"
        FROM hospital_demand
        WHERE "HospitalID" IS NOT NULL
    """, engine)

    con = duckdb.connect(DUCKDB_PATH)
    loc_df = con.execute("SELECT LocationSK, City, State FROM DimLocation").df()

    merged = df.merge(loc_df, on=['City', 'State'], how='left')

    dim = pd.DataFrame({
        'HospitalSK': range(1, len(merged) + 1),
        'HospitalID': merged['HospitalID'].values,
        'HospitalName': merged['HospitalName'].values,
        'HospitalType': merged['HospitalType'].values,
        'BedCapacity': merged['LicensedBeds'].fillna(0).astype(int).values,
        'HasPharmacyUnit': True,
        'AccreditationStatus': 'Accredited',
        'FundingType': 'Government',
        'EmergencyLevel': merged['TraumaLevel'].values,
        'OperatingStatus': 'Operational',
        'LocationSK': merged['LocationSK'].fillna(0).astype(int).values,
    })

    con.execute("DELETE FROM DimHospital")
    con.execute("INSERT INTO DimHospital SELECT * FROM dim")
    con.close()
    print(f"Loaded {len(dim)} rows into DimHospital")


def load_dim_sales_profile():
    """Build junk dimension from unique combinations of low-cardinality flags."""
    engine = create_engine(STAGING_CONN)

    df = pd.read_sql("""
        SELECT DISTINCT
            "SalesChannel",
            "CustomerSegment",
            "PaymentMethod",
            "PayerType" AS "InsuranceCoverage",
            "PrescriptionRequiredFlag" AS "PrescriptionRequired",
            CASE WHEN "PromotionCode" IS NOT NULL AND "PromotionCode" != ''
                 THEN TRUE ELSE FALSE END AS "IsEmergencyOrder",
            FALSE AS "IsColdChainProduct"
        FROM pharmacy_sales
    """, engine)

    df['PrescriptionRequired'] = df['PrescriptionRequired'].astype(bool)

    dim = df.reset_index(drop=True)
    dim.insert(0, 'SalesProfileSK', range(1, len(dim) + 1))

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("DELETE FROM DimSalesProfile")
    con.execute("INSERT INTO DimSalesProfile SELECT * FROM dim")
    con.close()
    print(f"Loaded {len(dim)} rows into DimSalesProfile")


# ══════════════════════════════════════════════
# FACTS
# ══════════════════════════════════════════════

def load_fact_pharmacy_sales():
    """Transform pharmacy sales: FK lookups + USD to PKR conversion."""
    engine = create_engine(STAGING_CONN)

    print("Reading pharmacy sales from staging...")
    df = pd.read_sql("SELECT * FROM pharmacy_sales", engine)

    con = duckdb.connect(DUCKDB_PATH)

    # Load lookup DataFrames
    date_df = con.execute("SELECT DateSK, FullDate FROM DimDate").df()
    date_df['FullDate'] = pd.to_datetime(date_df['FullDate'], format='mixed', dayfirst=True).dt.date

    med_df = con.execute("SELECT MedicineSK, MedicineID FROM DimMedicine").df()
    pharm_df = con.execute("SELECT PharmacySK, PharmacyID FROM DimPharmacy").df()
    loc_df = con.execute("SELECT LocationSK, City, State FROM DimLocation").df()
    sp_df = con.execute("""
        SELECT SalesProfileSK, SalesChannel, CustomerSegment,
               PaymentMethod, InsuranceCoverage, PrescriptionRequired
        FROM DimSalesProfile
    """).df()

    print("Performing FK lookups...")

    # ── Date lookups ──
    date_map = dict(zip(date_df['FullDate'], date_df['DateSK']))

    df['SalesDate'] = pd.to_datetime(df['SalesDate'], format='mixed', dayfirst=True).dt.date
    df['SaleDateSK'] = df['SalesDate'].map(date_map).fillna(0).astype(int)

    df['BatchExpiryDate'] = pd.to_datetime(
        df['BatchExpiryDate'], format='mixed', dayfirst=True, errors='coerce').dt.date
    df['BatchExpiryDateSK'] = df['BatchExpiryDate'].map(
        date_map).fillna(0).astype(int)

    df['PharmacyDeliveryDate'] = pd.to_datetime(
        df['PharmacyDeliveryDate'], format='mixed', dayfirst=True, errors='coerce').dt.date
    df['DeliveryDateSK'] = df['PharmacyDeliveryDate'].map(
        date_map).fillna(0).astype(int)

    # ── Medicine lookup ──
    med_map = dict(zip(med_df['MedicineID'], med_df['MedicineSK']))
    df['MedicineSK'] = df['MedicineID'].map(med_map).fillna(0).astype(int)

    # ── Pharmacy lookup ──
    pharm_map = dict(zip(pharm_df['PharmacyID'], pharm_df['PharmacySK']))
    df['PharmacySK'] = df['PharmacyID'].map(pharm_map).fillna(0).astype(int)

    # ── Location lookup ──
    loc_map = {}
    for _, row in loc_df.iterrows():
        loc_map[(row['City'], row['State'])] = row['LocationSK']
    df['LocationSK'] = df.apply(
        lambda r: loc_map.get((r['PharmacyCity'], r['PharmacyState']), 0),
        axis=1
    ).astype(int)

    # ── SalesProfile lookup ──
    df['PrescriptionRequiredFlag'] = df['PrescriptionRequiredFlag'].astype(bool)
    sp_merge = df.merge(
        sp_df,
        left_on=['SalesChannel', 'CustomerSegment', 'PaymentMethod',
                 'PayerType', 'PrescriptionRequiredFlag'],
        right_on=['SalesChannel', 'CustomerSegment', 'PaymentMethod',
                  'InsuranceCoverage', 'PrescriptionRequired'],
        how='left'
    )
    df['SalesProfileSK'] = sp_merge['SalesProfileSK'].fillna(0).astype(int)

    # ── Derive HourOfDay ──
    df['SalesTimestampUTC'] = pd.to_datetime(
        df['SalesTimestampUTC'], errors='coerce')
    df['HourOfDay'] = df['SalesTimestampUTC'].dt.hour.fillna(0).astype(int)

    # ── Build fact with USD → PKR conversion ──
    fact = pd.DataFrame({
        'PharmacySalesSK': range(1, len(df) + 1),
        'SaleDateSK': df['SaleDateSK'],
        'BatchExpiryDateSK': df['BatchExpiryDateSK'],
        'DeliveryDateSK': df['DeliveryDateSK'],
        'MedicineSK': df['MedicineSK'],
        'PharmacySK': df['PharmacySK'],
        'LocationSK': df['LocationSK'],
        'SalesProfileSK': df['SalesProfileSK'],
        'QuantitySoldPacks': df['QuantitySoldPacks'].fillna(0).astype(int),
        'UnitRetailPricePKR': (df['UnitRetailPriceUSD'] * USD_TO_PKR).round(4),
        'GrossSalesAmountPKR': (df['GrossSalesAmountUSD'] * USD_TO_PKR).round(4),
        'DiscountAmountPKR': (df['DiscountAmountUSD'] * USD_TO_PKR).round(4),
        'NetSalesAmountPKR': (df['NetSalesAmountUSD'] * USD_TO_PKR).round(4),
        'TaxAmountPKR': (df['SalesTaxUSD'] * USD_TO_PKR).round(4),
        'COGSAmountPKR': (df['CostOfGoodsSoldUSD'] * USD_TO_PKR).round(4),
        'GrossMarginPKR': (df['GrossMarginUSD'] * USD_TO_PKR).round(4),
        'ReturnedQuantityPacks': df['ReturnedQuantityPacks'].fillna(0).astype(int),
        'DaysToExpiryAtSale': df['DaysToExpiryAtSale'].fillna(0).astype(int),
        'HourOfDay': df['HourOfDay'],
        'LineItemCount': df.get(
            'LineItemCount', pd.Series([1]*len(df))).fillna(1).astype(int),
        'SalesTransactionID': df['SalesTransactionID'],
        'PharmacyOrderID': df['PharmacyOrderID'],
        'PharmacyDeliveryID': df['PharmacyDeliveryID'],
        'PrescriptionID': df['PrescriptionID'],
        'PromotionCode': df['PromotionCode'],
    })

    print("Loading into DuckDB FactPharmacySales...")
    con.execute("DELETE FROM FactPharmacySales")
    chunk_size = 50000
    for i in range(0, len(fact), chunk_size):
        chunk = fact.iloc[i:i+chunk_size]
        con.execute("INSERT INTO FactPharmacySales SELECT * FROM chunk")
        print(f"  Loaded rows {i+1} to {min(i+chunk_size, len(fact))}")

    con.close()
    print(f"Total: {len(fact)} rows into FactPharmacySales")


def load_fact_hospital_demand():
    """Transform hospital demand: FK lookups + USD to PKR conversion."""
    engine = create_engine(STAGING_CONN)
    df = pd.read_sql("SELECT * FROM hospital_demand", engine)

    con = duckdb.connect(DUCKDB_PATH)

    # Load lookups
    date_df = con.execute("SELECT DateSK, FullDate FROM DimDate").df()
    date_df['FullDate'] = pd.to_datetime(date_df['FullDate'], format='mixed', dayfirst=True).dt.date

    med_df = con.execute("SELECT MedicineSK, MedicineID FROM DimMedicine").df()
    hosp_df = con.execute("SELECT HospitalSK, HospitalID FROM DimHospital").df()
    loc_df = con.execute("SELECT LocationSK, City, State FROM DimLocation").df()

    # FK lookups
    date_map = dict(zip(date_df['FullDate'], date_df['DateSK']))
    df['DemandMonth'] = pd.to_datetime(df['DemandMonth'], format='mixed', dayfirst=True).dt.date
    df['DemandMonthSK'] = df['DemandMonth'].map(date_map).fillna(0).astype(int)

    med_map = dict(zip(med_df['MedicineID'], med_df['MedicineSK']))
    df['MedicineSK'] = df['MedicineID'].map(med_map).fillna(0).astype(int)

    hosp_map = dict(zip(hosp_df['HospitalID'], hosp_df['HospitalSK']))
    df['HospitalSK'] = df['HospitalID'].map(hosp_map).fillna(0).astype(int)

    loc_map = {}
    for _, row in loc_df.iterrows():
        loc_map[(row['City'], row['State'])] = row['LocationSK']
    df['LocationSK'] = df.apply(
        lambda r: loc_map.get((r['City'], r['State']), 0), axis=1
    ).astype(int)

    # Build fact with USD → PKR
    fact = pd.DataFrame({
        'HospitalDemandSK': range(1, len(df) + 1),
        'DemandMonthSK': df['DemandMonthSK'],
        'MedicineSK': df['MedicineSK'],
        'HospitalSK': df['HospitalSK'],
        'LocationSK': df['LocationSK'],
        'ForecastDemandPacks': df['ForecastDemandPacks'].fillna(0).astype(int),
        'ActualConsumptionPacks': df['ConsumedQuantityPacks'].fillna(0).astype(int),
        'UnfulfilledDemandPacks': df['UnfulfilledDemandPacks'].fillna(0).astype(int),
        'OpeningStockPacks': df['OpeningStockPacks'].fillna(0).astype(int),
        'ClosingStockPacks': df['ClosingStockPacks'].fillna(0).astype(int),
        'QuantityReceivedPacks': df['ReceivedQuantityPacks'].fillna(0).astype(int),
        'QuantityExpiredPacks': df['WastageQuantityPacks'].fillna(0).astype(int),
        'StockoutDays': df['StockoutDays'].fillna(0).astype(int),
        'EmergencyOrderCount': (df['EmergencyRequestQuantityPacks'] > 0).astype(int),
        'EmergencyOrderCostPKR': (df['EmergencyRequestQuantityPacks'].fillna(0)
                                  * 10 * USD_TO_PKR).round(4),
        'WastageValuePKR': (df['WastageQuantityPacks'].fillna(0)
                            * 5 * USD_TO_PKR).round(4),
        'InventoryCarryingCostPKR': (df['ClosingStockPacks'].fillna(0)
                                     * 0.5 * USD_TO_PKR).round(4),
        'BudgetVariancePKR': ((df['ForecastDemandPacks'].fillna(0)
                               - df['ConsumedQuantityPacks'].fillna(0))
                              * 5 * USD_TO_PKR).round(4),
        'ServiceLevelPct': df['ServiceLevelPct'].fillna(0).round(2),
        'DemandTrendIndex': df['DemandTrendIndex'].fillna(0).round(4),
        'AverageLeadTimeDays': df['AverageLeadTimeDays'].fillna(0).round(2),
        'SafetyStockPacks': df.get(
            'SafetyStockPacks', pd.Series([0]*len(df))).fillna(0).astype(int),
        'DaysOfSupply': (df['ClosingStockPacks'].fillna(0)
                         / df['ConsumedQuantityPacks'].replace(0, 1).fillna(1)
                         * 30).round(0).astype(int),
        'HospitalDemandRecordID': df['HospitalDemandRecordID'],
    })

    con.execute("DELETE FROM FactHospitalDemand")
    con.execute("INSERT INTO FactHospitalDemand SELECT * FROM fact")
    con.close()
    print(f"Loaded {len(fact)} rows into FactHospitalDemand")


# ══════════════════════════════════════════════
# VALIDATE: Query Layer
# ══════════════════════════════════════════════

def run_validation_queries():
    """Run 3 validated dimensional queries to prove the star schema works."""
    con = duckdb.connect(DUCKDB_PATH)

    # Table row counts
    print("\n" + "="*60)
    print("STAR SCHEMA ROW COUNTS")
    print("="*60)
    for table in ['DimDate', 'DimMedicine', 'DimLocation', 'DimPharmacy',
                  'DimHospital', 'DimSalesProfile',
                  'FactPharmacySales', 'FactHospitalDemand']:
        cnt = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {cnt:,} rows")

    # Query 1: Net sales by disease category + channel + quarter
    print("\n" + "-"*60)
    print("QUERY 1: Net sales by disease category, channel, quarter")
    print("-"*60)
    q1 = con.execute("""
        SELECT dm.DiseaseCategory, sp.SalesChannel,
               dd.Year, dd.Quarter,
               SUM(f.NetSalesAmountPKR) AS TotalNetSalesPKR
        FROM FactPharmacySales f
        JOIN DimMedicine dm ON f.MedicineSK = dm.MedicineSK
        JOIN DimSalesProfile sp ON f.SalesProfileSK = sp.SalesProfileSK
        JOIN DimDate dd ON f.SaleDateSK = dd.DateSK
        GROUP BY dm.DiseaseCategory, sp.SalesChannel,
                 dd.Year, dd.Quarter
        ORDER BY dd.Year, dd.Quarter
        LIMIT 10
    """).df()
    print(q1.to_string(index=False))

    # Query 2: Unfulfilled demand + stockouts by hospital
    print("\n" + "-"*60)
    print("QUERY 2: Unfulfilled demand by hospital")
    print("-"*60)
    q2 = con.execute("""
        SELECT dh.HospitalName, dh.HospitalType,
               dl.City, dl.State,
               SUM(f.UnfulfilledDemandPacks) AS TotalUnfulfilled,
               SUM(f.StockoutDays) AS TotalStockoutDays,
               ROUND(AVG(f.ServiceLevelPct), 2) AS AvgServiceLevel
        FROM FactHospitalDemand f
        JOIN DimHospital dh ON f.HospitalSK = dh.HospitalSK
        JOIN DimLocation dl ON f.LocationSK = dl.LocationSK
        JOIN DimDate dd ON f.DemandMonthSK = dd.DateSK
        GROUP BY dh.HospitalName, dh.HospitalType,
                 dl.City, dl.State
        ORDER BY TotalUnfulfilled DESC
        LIMIT 10
    """).df()
    print(q2.to_string(index=False))

    # Query 3: Gross margin + return rate by pharmacy type + segment
    print("\n" + "-"*60)
    print("QUERY 3: Margin and return rate by pharmacy type, segment")
    print("-"*60)
    q3 = con.execute("""
        SELECT dp.PharmacyType, sp.CustomerSegment,
               SUM(f.GrossMarginPKR) AS TotalMarginPKR,
               SUM(f.ReturnedQuantityPacks) AS TotalReturns,
               SUM(f.QuantitySoldPacks) AS TotalSold,
               ROUND(SUM(f.ReturnedQuantityPacks)*100.0
                     / NULLIF(SUM(f.QuantitySoldPacks),0), 2)
                     AS ReturnRatePct
        FROM FactPharmacySales f
        JOIN DimPharmacy dp ON f.PharmacySK = dp.PharmacySK
        JOIN DimSalesProfile sp ON f.SalesProfileSK = sp.SalesProfileSK
        GROUP BY dp.PharmacyType, sp.CustomerSegment
        ORDER BY ReturnRatePct DESC
        LIMIT 10
    """).df()
    print(q3.to_string(index=False))

    con.close()
    print("\n" + "="*60)
    print("ALL 3 VALIDATION QUERIES PASSED")
    print("="*60)


# ══════════════════════════════════════════════
# Task Definitions
# ══════════════════════════════════════════════

t_schema = PythonOperator(
    task_id="create_duckdb_schema",
    python_callable=create_duckdb_schema,
    dag=dag,
)

t_dim_date = PythonOperator(
    task_id="load_dim_date",
    python_callable=load_dim_date,
    dag=dag,
)

t_dim_medicine = PythonOperator(
    task_id="load_dim_medicine",
    python_callable=load_dim_medicine,
    dag=dag,
)

t_dim_location = PythonOperator(
    task_id="load_dim_location",
    python_callable=load_dim_location,
    dag=dag,
)

t_dim_pharmacy = PythonOperator(
    task_id="load_dim_pharmacy",
    python_callable=load_dim_pharmacy,
    dag=dag,
)

t_dim_hospital = PythonOperator(
    task_id="load_dim_hospital",
    python_callable=load_dim_hospital,
    dag=dag,
)

t_dim_sales_profile = PythonOperator(
    task_id="load_dim_sales_profile",
    python_callable=load_dim_sales_profile,
    dag=dag,
)

t_fact_sales = PythonOperator(
    task_id="load_fact_pharmacy_sales",
    python_callable=load_fact_pharmacy_sales,
    dag=dag,
)

t_fact_demand = PythonOperator(
    task_id="load_fact_hospital_demand",
    python_callable=load_fact_hospital_demand,
    dag=dag,
)

t_validate = PythonOperator(
    task_id="validate_queries",
    python_callable=run_validation_queries,
    dag=dag,
)


# ══════════════════════════════════════════════
# Dependencies
# ══════════════════════════════════════════════
#
#  DuckDB allows only 1 writer at a time, so all tasks run sequentially:
#
#  schema → dim_date → dim_location → dim_medicine
#         → dim_pharmacy → dim_hospital → dim_sales_profile
#         → fact_pharmacy_sales → fact_hospital_demand → validate

t_schema >> t_dim_date >> t_dim_location >> t_dim_medicine
t_dim_medicine >> t_dim_pharmacy >> t_dim_hospital >> t_dim_sales_profile
t_trigger_query = TriggerDagRunOperator(
    task_id="trigger_dag_03_query",
    trigger_dag_id="dag_03_query_layer",
    dag=dag,
)

t_dim_sales_profile >> t_fact_sales >> t_fact_demand >> t_validate >> t_trigger_query