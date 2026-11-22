import os
import json
import numpy as np
import pandas as pd
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Auto detect environment: Docker (/opt/airflow) vs local Windows
if os.path.isdir("/opt/airflow/data"):
    BASE_DIR = "/opt/airflow"
else:
    BASE_DIR = r"D:\IBA\Semester_1\DataWarehouseAndDataAnalytics\Project\clarity-pharma-dwh"

RAW_DIR = os.path.join(BASE_DIR, "data", "baseline")   # Put original 7 CSVs here
OUT_BASE = os.path.join(BASE_DIR, "data")

# Files 01-06 output as .xlsx, file 07 stays .csv for streaming
XLSX_FILES = {
    "01_master_data",
    "02_manufacturing_data",
    "03_manufacturer_delivery_data",
    "04_warehouse_data",
    "05_pharmacy_sales_data",
    "06_hospital_demand_data",
}

BATCH_CONFIGS = {
    1: {
        "name": "batch_1",
        "start_date": "2026-01-01",
        "end_date": "2026-06-30",
        "months": ["01/01/2026", "01/02/2026", "01/03/2026",
                    "01/04/2026", "01/05/2026", "01/06/2026"],
        "rows": {
            "01_master_data.csv": 2,
            "02_manufacturing_data.csv": 120,
            "03_manufacturer_delivery_data.csv": 220,
            "04_warehouse_data.csv": 24000,
            "05_pharmacy_sales_data.csv": 100000,
            "06_hospital_demand_data.csv": 4536,   # 12 hospitals * 63 meds * 6 months
            "07_iot_streaming_data.csv": 60000,
        },
    },
    2: {
        "name": "batch_2",
        "start_date": "2026-07-01",
        "end_date": "2026-12-31",
        "months": ["01/07/2026", "01/08/2026", "01/09/2026",
                    "01/10/2026", "01/11/2026", "01/12/2026"],
        "rows": {
            "01_master_data.csv": 2,
            "02_manufacturing_data.csv": 120,
            "03_manufacturer_delivery_data.csv": 220,
            "04_warehouse_data.csv": 24000,
            "05_pharmacy_sales_data.csv": 100000,
            "06_hospital_demand_data.csv": 4536,
            "07_iot_streaming_data.csv": 60000,
        },
    },
}


def random_dates(start_str: str, end_str: str, n: int, rng: np.random.Generator) -> pd.DatetimeIndex:
    start_u = pd.Timestamp(start_str).value // 10**9
    end_u = pd.Timestamp(end_str).value // 10**9
    return pd.to_datetime(rng.integers(start_u, end_u + 1, size=n), unit="s")


def get_cumulative_df(filename: str, up_to_batch: int) -> pd.DataFrame:
    """Loads baseline + any previously generated batches so IDs never collide."""
    base_csv = os.path.join(RAW_DIR, filename)
    dfs = [pd.read_csv(base_csv, low_memory=False)]
    stem = filename.replace(".csv", "")
    for b in range(1, up_to_batch):
        # Check for .xlsx first (files 01-06), then .csv (file 07)
        prev_xlsx = os.path.join(OUT_BASE, f"batch_{b}", f"{stem}.xlsx")
        prev_csv = os.path.join(OUT_BASE, f"batch_{b}", filename)
        if os.path.exists(prev_xlsx):
            dfs.append(pd.read_excel(prev_xlsx, engine="openpyxl"))
        elif os.path.exists(prev_csv):
            dfs.append(pd.read_csv(prev_csv, low_memory=False))
    return pd.concat(dfs, ignore_index=True)


def save_output(df: pd.DataFrame, out_dir: str, csv_filename: str):
    """Save as .xlsx for files 01-06, .csv for file 07."""
    stem = csv_filename.replace(".csv", "")
    if stem in XLSX_FILES:
        out_path = os.path.join(out_dir, f"{stem}.xlsx")
        df.to_excel(out_path, index=False, engine="openpyxl")
    else:
        out_path = os.path.join(out_dir, csv_filename)
        df.to_csv(out_path, index=False)
    return out_path


def generate_batch(batch_num: int, seed: int = 42, inject_iot_errors_pct: float = 0.015) -> dict:
    """
    Generates a future-dated synthetic batch across all 7 Clarity Pharma files
    while preserving distributions, ID sequences, and foreign keys.
    Can be called standalone or wrapped inside an Airflow PythonOperator.
    """
    cfg = BATCH_CONFIGS[batch_num]
    rng = np.random.default_rng(seed + batch_num)
    out_dir = os.path.join(OUT_BASE, cfg["name"])
    os.makedirs(out_dir, exist_ok=True)

    stats = []
    start_dt, end_dt = cfg["start_date"], cfg["end_date"]

    # --------------------------------------------------------------------------
    # 1. 01_master_data (Add 2 new SKUs per batch + SCD Type 2 price changes in batch_2)
    # --------------------------------------------------------------------------
    f1 = "01_master_data.csv"
    df1 = get_cumulative_df(f1, batch_num)
    n1 = cfg["rows"][f1]
    max_med = int(df1["MedicineID"].str.replace("MED", "").astype(int).max())

    # --- New medicines (both batches) ---
    syn1 = df1.sample(n=n1, replace=True, random_state=seed + batch_num).copy().reset_index(drop=True)
    for i in range(n1):
        new_num = max_med + i + 1
        syn1.loc[i, "MedicineID"] = f"MED{new_num:03d}"
        syn1.loc[i, "InternalSKU"] = f"SKU-{new_num:04d}"
        syn1.loc[i, "GTIN14"] = int(df1["GTIN14"].max()) + (i + 1) * 10
        syn1.loc[i, "FictionalBrandName"] = f"{syn1.loc[i, 'FictionalBrandName']}-XR{batch_num}"
        syn1.loc[i, "EffectiveFromDate"] = start_dt

    # --- SCD Type 2: In batch_2, also include 2 existing medicines with price changes ---
    if batch_num == 2:
        scd_meds = df1[df1["MedicineID"].isin(["MED010", "MED025"])].copy()
        if len(scd_meds) > 0:
            # Bump prices by 8-15% to simulate a price revision
            price_bump = rng.uniform(1.08, 1.15, size=len(scd_meds))
            scd_meds["WholesalePriceUSD"] = (scd_meds["WholesalePriceUSD"] * price_bump).round(2)
            scd_meds["RecommendedRetailPriceUSD"] = (scd_meds["RecommendedRetailPriceUSD"] * price_bump).round(2)
            scd_meds["EffectiveFromDate"] = "2026-04-01"
            scd_meds["ProductStatus"] = "Active"
            syn1 = pd.concat([syn1, scd_meds], ignore_index=True)

    save_output(syn1, out_dir, f1)
    stats.append({"batch": cfg["name"], "file": f1, "baseline_rows": len(df1), "generated_rows": len(syn1)})

    # --------------------------------------------------------------------------
    # 2. 02_manufacturing_data (DD/MM/YYYY dates)
    # --------------------------------------------------------------------------
    f2 = "02_manufacturing_data.csv"
    df2 = get_cumulative_df(f2, batch_num)
    n2 = cfg["rows"][f2]
    max_mo = int(df2["ManufacturingOrderID"].str.split("-").str[-1].astype(int).max())
    syn2 = df2.sample(n=n2, replace=True, random_state=seed + batch_num + 1).copy().reset_index(drop=True)

    order_dts = random_dates(start_dt, end_dt, n2, rng)
    lead_days = pd.to_timedelta(syn2["ManufacturingLeadTimeDays"].fillna(21), unit="D")
    comp_dts = pd.DatetimeIndex(order_dts + lead_days)
    expiry_dts = pd.DatetimeIndex(comp_dts + pd.to_timedelta(rng.integers(540, 1080, size=n2), unit="D"))

    syn2["ManufacturingOrderID"] = [f"MO-2026-{max_mo + i + 1:05d}" for i in range(n2)]
    syn2["ProductionBatchID"] = [f"PB-2026-{max_mo + i + 1:05d}" for i in range(n2)]
    syn2["OrderDate"] = order_dts.strftime("%d/%m/%Y")
    syn2["PlannedStartDate"] = (order_dts + pd.Timedelta(days=2)).strftime("%d/%m/%Y")
    syn2["ActualStartDate"] = (order_dts + pd.Timedelta(days=2)).strftime("%d/%m/%Y")
    syn2["PlannedCompletionDate"] = comp_dts.strftime("%d/%m/%Y")
    syn2["ActualCompletionDate"] = comp_dts.strftime("%d/%m/%Y")
    syn2["QualitySampleDate"] = (comp_dts + pd.Timedelta(days=1)).strftime("%d/%m/%Y")
    syn2["BatchReleaseDate"] = (comp_dts + pd.Timedelta(days=3)).strftime("%d/%m/%Y")
    syn2["ExpiryDate"] = expiry_dts.strftime("%d/%m/%Y")
    syn2["RecordCreatedTimestampUTC"] = order_dts.strftime("%Y-%m-%dT%H:%M:%SZ")

    jitter = rng.normal(1.0, 0.03, size=n2)
    syn2["ActualQuantityPacks"] = (syn2["ActualQuantityPacks"] * jitter).round().astype(int)
    syn2["ReleasedQuantityPacks"] = np.maximum(0, syn2["ActualQuantityPacks"] - syn2["RejectedQuantityPacks"])
    save_output(syn2, out_dir, f2)
    stats.append({"batch": cfg["name"], "file": f2, "baseline_rows": len(df2), "generated_rows": len(syn2)})

    # --------------------------------------------------------------------------
    # 3. 03_manufacturer_delivery_data (YYYY-MM-DD dates)
    # --------------------------------------------------------------------------
    f3 = "03_manufacturer_delivery_data.csv"
    df3 = get_cumulative_df(f3, batch_num)
    n3 = cfg["rows"][f3]
    max_shp = int(df3["ShipmentID"].str.split("-").str[-1].astype(int).max())
    syn3 = df3.sample(n=n3, replace=True, random_state=seed + batch_num + 2).copy().reset_index(drop=True)

    disp_dts = random_dates(start_dt, end_dt, n3, rng)
    transit_days = pd.to_timedelta(np.ceil(syn3["TransitHours"].fillna(48) / 24.0), unit="D")
    deliv_dts = pd.DatetimeIndex(disp_dts + transit_days)

    syn3["ShipmentID"] = [f"MFG-SHP-{max_shp + i + 1:07d}" for i in range(n3)]
    syn3["ShipmentLineNumber"] = 1
    syn3["ManufacturingOrderID"] = rng.choice(syn2["ManufacturingOrderID"], size=n3)
    syn3["ProductionBatchID"] = rng.choice(syn2["ProductionBatchID"], size=n3)
    syn3["DispatchDate"] = disp_dts.strftime("%Y-%m-%d")
    syn3["PromisedDeliveryDate"] = (disp_dts + pd.Timedelta(days=3)).strftime("%Y-%m-%d")
    syn3["ActualDeliveryDate"] = deliv_dts.strftime("%Y-%m-%d")
    save_output(syn3, out_dir, f3)
    stats.append({"batch": cfg["name"], "file": f3, "baseline_rows": len(df3), "generated_rows": len(syn3)})

    # --------------------------------------------------------------------------
    # 4. 04_warehouse_data (YYYY-MM-DD dates)
    # --------------------------------------------------------------------------
    f4 = "04_warehouse_data.csv"
    df4 = get_cumulative_df(f4, batch_num)
    n4 = cfg["rows"][f4]
    max_inv = int(df4["InventorySnapshotID"].str.split("-").str[-1].astype(int).max())
    syn4 = df4.sample(n=n4, replace=True, random_state=seed + batch_num + 3).copy().reset_index(drop=True)

    snap_dts = random_dates(start_dt, end_dt, n4, rng)
    days_left = rng.integers(15, 900, size=n4)
    syn4["InventorySnapshotID"] = [f"INV-{max_inv + i + 1:09d}" for i in range(n4)]
    syn4["SnapshotDate"] = snap_dts.strftime("%Y-%m-%d")
    syn4["DaysToExpiry"] = days_left
    syn4["BatchExpiryDate"] = pd.DatetimeIndex(snap_dts + pd.to_timedelta(days_left, unit="D")).strftime("%Y-%m-%d")
    syn4["NearExpiryFlag"] = (days_left <= 90).astype(int)
    # FK link: point to new batch_N shipments and production batches
    syn4["InboundShipmentID"] = rng.choice(syn3["ShipmentID"], size=n4)
    syn4["ProductionBatchID"] = rng.choice(syn2["ProductionBatchID"], size=n4)
    save_output(syn4, out_dir, f4)
    stats.append({"batch": cfg["name"], "file": f4, "baseline_rows": len(df4), "generated_rows": len(syn4)})

    # --------------------------------------------------------------------------
    # 5. 05_pharmacy_sales_data (DD/MM/YYYY dates)
    # --------------------------------------------------------------------------
    f5 = "05_pharmacy_sales_data.csv"
    df5 = get_cumulative_df(f5, batch_num)
    n5 = cfg["rows"][f5]
    max_sale = int(df5["SalesTransactionID"].str.split("-").str[-1].astype(int).max())
    syn5 = df5.sample(n=n5, replace=True, random_state=seed + batch_num + 4).copy().reset_index(drop=True)

    sale_dts = random_dates(start_dt, end_dt, n5, rng)
    syn5["SalesTransactionID"] = [f"SALE-{max_sale + i + 1:09d}" for i in range(n5)]
    syn5["SalesDate"] = sale_dts.strftime("%d/%m/%Y")
    syn5["SalesTimestampUTC"] = sale_dts.strftime("%Y-%m-%dT%H:%M:%SZ")
    syn5["PharmacyDeliveryDate"] = (sale_dts + pd.Timedelta(days=1)).strftime("%d/%m/%Y")
    syn5["BatchExpiryDate"] = pd.DatetimeIndex(sale_dts + pd.to_timedelta(syn5["DaysToExpiryAtSale"], unit="D")).strftime("%d/%m/%Y")
    # FK link: point to new batch_N production batches
    syn5["ProductionBatchID"] = rng.choice(syn2["ProductionBatchID"], size=n5)
    # SCD Type 2: apply same price bump to MED010 & MED025 sales in batch_2
    if batch_num == 2:
        mask = syn5["MedicineID"].isin(["MED010", "MED025"])
        syn5.loc[mask, "UnitRetailPriceUSD"] = (syn5.loc[mask, "UnitRetailPriceUSD"] * 1.10).round(2)

    # Recompute financial columns cleanly so accounting identities always hold
    syn5["GrossSalesAmountUSD"] = (syn5["QuantitySoldPacks"] * syn5["UnitRetailPriceUSD"]).round(2)
    syn5["NetSalesAmountUSD"] = (syn5["GrossSalesAmountUSD"] - syn5["DiscountAmountUSD"]).round(2)
    syn5["GrossMarginUSD"] = (syn5["NetSalesAmountUSD"] - syn5["CostOfGoodsSoldUSD"]).round(2)
    save_output(syn5, out_dir, f5)
    stats.append({"batch": cfg["name"], "file": f5, "baseline_rows": len(df5), "generated_rows": len(syn5)})

    # --------------------------------------------------------------------------
    # 6. 06_hospital_demand_data (Exact Hospital x Medicine x Month Panel)
    # --------------------------------------------------------------------------
    f6 = "06_hospital_demand_data.csv"
    df6 = get_cumulative_df(f6, batch_num)
    max_hdem = int(df6["HospitalDemandRecordID"].str.split("-").str[-1].astype(int).max())

    # Use last 3 historical months as structural template to preserve Hospital-Medicine pairs
    latest_pairs = df6.drop_duplicates(subset=["HospitalID", "MedicineID"], keep="last").copy()
    monthly_frames = []
    for m_str in cfg["months"]:
        m_df = latest_pairs.copy()
        m_df["DemandMonth"] = m_str
        scale = rng.normal(1.02, 0.05, size=len(m_df))  # slight realistic demand growth
        m_df["ForecastDemandPacks"] = np.maximum(1, (m_df["ForecastDemandPacks"] * scale).round()).astype(int)
        m_df["RequestedQuantityPacks"] = np.maximum(1, (m_df["RequestedQuantityPacks"] * scale).round()).astype(int)
        m_df["ConsumedQuantityPacks"] = np.maximum(0, (m_df["ConsumedQuantityPacks"] * scale).round()).astype(int)
        m_df["UnfulfilledDemandPacks"] = np.maximum(0, m_df["RequestedQuantityPacks"] - m_df["ConsumedQuantityPacks"])
        monthly_frames.append(m_df)

    syn6 = pd.concat(monthly_frames, ignore_index=True)
    syn6["HospitalDemandRecordID"] = [f"HDEM-{max_hdem + i + 1:08d}" for i in range(len(syn6))]
    save_output(syn6, out_dir, f6)
    stats.append({"batch": cfg["name"], "file": f6, "baseline_rows": len(df6), "generated_rows": len(syn6)})

    # --------------------------------------------------------------------------
    # 7. 07_iot_streaming_data (ISO Timestamps + Controlled Dirty Rows) — stays .csv
    # --------------------------------------------------------------------------
    f7 = "07_iot_streaming_data.csv"
    df7 = get_cumulative_df(f7, batch_num)
    n7 = cfg["rows"][f7]
    max_iot = int(df7["SensorReadingID"].str.split("-").str[-1].astype(int).max())
    syn7 = df7.sample(n=n7, replace=True, random_state=seed + batch_num + 6).copy().reset_index(drop=True)

    evt_dts = random_dates(start_dt, end_dt, n7, rng).sort_values()
    latency_sec = rng.integers(1, 15, size=n7)
    ing_dts = evt_dts + pd.to_timedelta(latency_sec, unit="s")

    syn7["SensorReadingID"] = [f"IOTR-{max_iot + i + 1:010d}" for i in range(n7)]
    # FK link: point transit rows to new batch_N shipments
    transit_mask = syn7["ShipmentID"].notna()
    syn7.loc[transit_mask, "ShipmentID"] = rng.choice(syn3["ShipmentID"], size=transit_mask.sum())
    syn7["EventTimestampUTC"] = evt_dts.strftime("%Y-%m-%dT%H:%M:%SZ")
    syn7["IngestionTimestampUTC"] = ing_dts.strftime("%Y-%m-%dT%H:%M:%SZ")
    syn7["DataLatencySeconds"] = latency_sec
    # Force numeric — batch_1 dirty rows may have left "ERR_NAN" strings in sampled data
    syn7["TemperatureC"] = pd.to_numeric(syn7["TemperatureC"], errors="coerce")
    syn7["TemperatureC"] = (syn7["TemperatureC"].fillna(syn7["TemperatureC"].median()) + rng.normal(0, 0.25, size=n7)).round(2)

    # Recompute breach flags accurately
    temp_breach = (syn7["TemperatureC"] < syn7["StorageMinTempC"]) | (syn7["TemperatureC"] > syn7["StorageMaxTempC"])
    hum_breach = syn7["RelativeHumidityPct"] > syn7["MaxHumidityPct"]
    syn7["TemperatureBreachFlag"] = temp_breach.astype(int)
    syn7["HumidityBreachFlag"] = hum_breach.astype(int)
    syn7["ThresholdBreachFlag"] = (temp_breach | hum_breach).astype(int)

    # Inject ~1.5% malformed rows so Streaming Ingestion Dashboard captures real drops
    n_dirty = int(n7 * inject_iot_errors_pct)
    dirty_idx = rng.choice(n7, size=n_dirty, replace=False)
    half = n_dirty // 2
    syn7["DeviceID"] = syn7["DeviceID"].astype(object)
    syn7["TemperatureC"] = syn7["TemperatureC"].astype(object)
    syn7.loc[dirty_idx[:half], "DeviceID"] = None           # Null key violation
    syn7.loc[dirty_idx[half:], "TemperatureC"] = "ERR_NAN"  # Schema type mismatch

    syn7.to_csv(os.path.join(out_dir, f7), index=False)
    stats.append({
        "batch": cfg["name"],
        "file": f7,
        "baseline_rows": len(df7),
        "generated_rows": len(syn7),
        "injected_dirty_rows": n_dirty,
    })

    return {"batch": cfg["name"], "period": f"{start_dt} to {end_dt}", "files": stats}


if __name__ == "__main__":
    all_reports = []
    for b in [1, 2]:
        rep = generate_batch(b)
        all_reports.append(rep)
        print(f"\n=== Generated {rep['batch']} ({rep['period']}) ===")
        for item in rep["files"]:
            ext = ".xlsx" if item["file"].replace(".csv", "") in XLSX_FILES else ".csv"
            print(f"  {item['file'].replace('.csv', ext):40s} | +{item['generated_rows']:,} rows")

    report_path = os.path.join(OUT_BASE, "oversampling_summary.json")
    with open(report_path, "w") as f:
        json.dump(all_reports, f, indent=2)
    print(f"\nSaved summary statistics to {report_path}")