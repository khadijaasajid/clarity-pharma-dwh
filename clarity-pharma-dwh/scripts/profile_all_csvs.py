"""
Profile all 7 baseline CSV files - run this to see row counts, columns, date ranges, ID formats.
Usage: python scripts/profile_all_csvs.py
"""

import pandas as pd
import os

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "baseline")

FILES = {
    "01_master_data.csv": {"date_cols": [], "id_col": "MedicineID"},
    "02_manufacturing_data.csv": {"date_cols": ["OrderDate", "ActualCompletionDate"], "id_col": "ManufacturingOrderID"},
    "03_manufacturer_delivery_data.csv": {"date_cols": ["DispatchDate", "ActualDeliveryDate"], "id_col": "ShipmentID"},
    "04_warehouse_data.csv": {"date_cols": ["SnapshotDate"], "id_col": "InventorySnapshotID"},
    "05_pharmacy_sales_data.csv": {"date_cols": ["SalesDate"], "id_col": "SalesTransactionID"},
    "06_hospital_demand_data.csv": {"date_cols": ["DemandMonth"], "id_col": "HospitalDemandRecordID"},
    "07_iot_streaming_data.csv": {"date_cols": ["EventTimestampUTC"], "id_col": "SensorReadingID"},
}

print("=" * 80)
print("CLARITY PHARMA - BASELINE DATA PROFILE")
print("=" * 80)

for filename, info in FILES.items():
    filepath = os.path.join(BASE_DIR, filename)
    if not os.path.exists(filepath):
        print(f"\n[MISSING] {filename} not found in {BASE_DIR}")
        continue

    df = pd.read_csv(filepath)
    print(f"\n{'=' * 80}")
    print(f"FILE: {filename}")
    print(f"{'=' * 80}")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Column names:\n    {', '.join(df.columns.tolist())}")

    # ID column info
    id_col = info["id_col"]
    if id_col in df.columns:
        print(f"\n  ID Column ({id_col}):")
        print(f"    Sample: {df[id_col].iloc[:3].tolist()}")
        print(f"    Max:    {df[id_col].max()}")
        print(f"    Unique: {df[id_col].nunique()}")

    # Date columns
    for dc in info["date_cols"]:
        if dc in df.columns:
            print(f"\n  Date Column ({dc}):")
            print(f"    Min: {df[dc].min()}")
            print(f"    Max: {df[dc].max()}")
            print(f"    Sample: {df[dc].iloc[:3].tolist()}")

    # Key categorical columns
    for col in ["PlantID", "WarehouseID", "PharmacyID", "HospitalID", "MedicineID",
                 "SalesChannel", "DeviceID", "DestinationWarehouseID"]:
        if col in df.columns:
            uniq = sorted(df[col].dropna().unique())
            if len(uniq) <= 15:
                print(f"\n  {col} unique values ({len(uniq)}): {uniq}")
            else:
                print(f"\n  {col} unique count: {len(uniq)}")
                print(f"    Sample: {uniq[:5]} ...")

    # Numeric summary for key columns
    num_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
    if num_cols:
        print(f"\n  Numeric columns ({len(num_cols)}):")
        for nc in num_cols[:8]:
            print(f"    {nc}: min={df[nc].min():.2f}, max={df[nc].max():.2f}, mean={df[nc].mean():.2f}")
        if len(num_cols) > 8:
            print(f"    ... and {len(num_cols) - 8} more")

print(f"\n{'=' * 80}")
print("PROFILE COMPLETE")
print(f"{'=' * 80}")
