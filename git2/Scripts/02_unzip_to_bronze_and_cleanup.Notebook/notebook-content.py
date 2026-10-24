# Fabric notebook source


# MARKDOWN ********************

# # 02 - Unzip Landing Files to Bronze & 7-Day Retention Cleanup
# Run each cell interactively to see:
# 1. Pending files in `meta_file_control_log` with `status == 'DOWNLOADED'`
# 2. In-memory unzipping of the AEMO C/I/D CSV (`I == 'D'`) and preview of extracted rows
# 3. Idempotent Delta `MERGE` into `bronze_scada` on `(SETTLEMENTDATE, DUID)`
# 4. 7-day cleanup of old ZIP files from `Files/landing/` while preserving history in `meta_file_control_log`

# MARKDOWN ********************

# ### Cell 1: Imports & Lakehouse Configuration
# *(Note: Tagged as a Parameters cell so `notebookutils.notebook.run()` can pass `retention_days`)*

# PARAMETERS CELL ********************

import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# Parameter (can be overridden by Fabric pipeline or notebook.run)
retention_days = 7

NEM_TZ = timezone(timedelta(hours=10))
LAKEHOUSE_ROOT = Path("/lakehouse/default") if Path("/lakehouse/default").exists() else Path("./local_lakehouse")
LANDING_BASE_DIR = LAKEHOUSE_ROOT / "Files" / "landing"
TABLES_BASE_DIR = LAKEHOUSE_ROOT / "Tables"

CONTROL_LOG_TABLE_PATH = str(TABLES_BASE_DIR / "meta_file_control_log")
BRONZE_SCADA_TABLE_PATH = str(TABLES_BASE_DIR / "bronze_scada")
SCADA_PRIMARY_KEYS = ["SETTLEMENTDATE", "DUID"]

print(f"Control Log Table  : {CONTROL_LOG_TABLE_PATH}")
print(f"Bronze SCADA Table : {BRONZE_SCADA_TABLE_PATH}")
print(f"Retention Days     : {retention_days}")

# MARKDOWN ********************

# ### Cell 2: Query `meta_file_control_log` for Pending (`DOWNLOADED`) ZIP Files

# CELL ********************

if DeltaTable.is_deltatable(CONTROL_LOG_TABLE_PATH):
    dt_log = DeltaTable(CONTROL_LOG_TABLE_PATH)
    df_log = dt_log.to_pandas()
    pending_df = df_log[df_log["status"] == "DOWNLOADED"].copy()
    print(f"Found {len(pending_df)} pending ZIP file(s) with status == 'DOWNLOADED':")
    display(pending_df)
else:
    pending_df = pd.DataFrame()
    print("meta_file_control_log table does not exist yet.")

# MARKDOWN ********************

# ### Cell 3: Unzip Pending Files in Memory & Preview Extracted SCADA Rows
# Reads AEMO C/I/D CSV directly from the `.zip` archive (`skiprows=1, dtype=str`), filters to `I == 'D'`, and attaches `source_file` and `ingested_at_utc`.

# CELL ********************

def read_aemo_mmsdm_zip(zip_path: Path, ingested_at_utc: str) -> pd.DataFrame:
    df = pd.read_csv(zip_path, skiprows=1, dtype=str)
    df_data = df[df["I"] == "D"].copy()
    df_data["source_file"] = zip_path.name
    df_data["ingested_at_utc"] = ingested_at_utc

    if "SETTLEMENTDATE" in df_data.columns:
        df_data["settlement_date_part"] = (
            df_data["SETTLEMENTDATE"]
            .str.slice(0, 10)
            .str.replace("/", "-", regex=False)
        )
    else:
        df_data["settlement_date_part"] = "unknown"

    return df_data

now_utc = datetime.now(timezone.utc)
now_iso = now_utc.isoformat()
ingested_updates = []
batch_dfs = []

if not pending_df.empty:
    for _, row in pending_df.iterrows():
        zip_path = Path(row["landing_path"])
        if not zip_path.exists():
            print(f"[WARNING] Missing ZIP file at {zip_path}")
            continue

        df_file = read_aemo_mmsdm_zip(zip_path, ingested_at_utc=now_iso)
        batch_dfs.append(df_file)
        ingested_updates.append({
            "file_name": row["file_name"],
            "status": "INGESTED_TO_BRONZE",
            "ingested_at_utc": now_iso,
            "bronze_row_count": int(len(df_file)),
        })
        print(f"Unzipped {zip_path.name}: {len(df_file):,} 'D' rows")

if batch_dfs:
    combined_df = pd.concat(batch_dfs, ignore_index=True)
    print(f"\nTotal extracted rows across {len(batch_dfs)} file(s): {len(combined_df):,}")
    display(combined_df.head(10))
else:
    combined_df = pd.DataFrame()
    print("No rows extracted.")

# MARKDOWN ********************

# ### Cell 4: Upsert into `bronze_scada` Delta Table & Mark `INGESTED_TO_BRONZE`
# Merges on `(SETTLEMENTDATE, DUID)` so overlapping intervals never create duplicate rows in Bronze.

# CELL ********************

total_rows_upserted = 0

if not combined_df.empty:
    available_keys = [k for k in SCADA_PRIMARY_KEYS if k in combined_df.columns]
    if available_keys:
        combined_df = combined_df.drop_duplicates(subset=available_keys, keep="last")

    TABLES_BASE_DIR.mkdir(parents=True, exist_ok=True)

    if not DeltaTable.is_deltatable(BRONZE_SCADA_TABLE_PATH):
        print("Creating new bronze_scada Delta table...")
        write_deltalake(
            BRONZE_SCADA_TABLE_PATH,
            combined_df,
            mode="overwrite",
            partition_by=["settlement_date_part"],
        )
    else:
        print("Merging into existing bronze_scada Delta table...")
        dt_bronze = DeltaTable(BRONZE_SCADA_TABLE_PATH)
        predicate = " AND ".join([f"target.{k} = source.{k}" for k in available_keys])
        (
            dt_bronze.merge(
                source=pa.Table.from_pandas(combined_df, preserve_index=False),
                predicate=predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    total_rows_upserted = len(combined_df)

    # Update meta_file_control_log status -> INGESTED_TO_BRONZE
    df_updates = pd.DataFrame(ingested_updates)
    dt_log = DeltaTable(CONTROL_LOG_TABLE_PATH)
    (
        dt_log.merge(
            source=pa.Table.from_pandas(df_updates, preserve_index=False),
            predicate="target.file_name = source.file_name",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update({
            "status": "source.status",
            "ingested_at_utc": "source.ingested_at_utc",
            "bronze_row_count": "source.bronze_row_count",
        })
        .execute()
    )
    print(f"Upserted {total_rows_upserted:,} rows to bronze_scada and marked {len(ingested_updates)} file(s) as INGESTED_TO_BRONZE.")
else:
    print("Nothing to upsert to bronze_scada.")

# MARKDOWN ********************

# ### Cell 5: Step 3 — Delete ZIP Files Older Than 7 Days from Landing
# Deletes physical `.zip` files from `Files/landing/` where `status == 'INGESTED_TO_BRONZE'` and `landing_date_folder < today - 7 days`, then updates `meta_file_control_log` to `PURGED_FROM_LANDING`.

# CELL ********************

cutoff_date_str = (now_utc.astimezone(NEM_TZ) - timedelta(days=int(retention_days))).strftime("%Y-%m-%d")
print(f"Checking for ingested Landing ZIPs with date folder < {cutoff_date_str} ({retention_days}-day retention)...")

purged_updates = []
folders_removed = 0

if DeltaTable.is_deltatable(CONTROL_LOG_TABLE_PATH):
    dt_log = DeltaTable(CONTROL_LOG_TABLE_PATH)
    df_log = dt_log.to_pandas()

    eligible_df = df_log[
        (df_log["status"] == "INGESTED_TO_BRONZE")
        & (df_log["landing_date_folder"] < cutoff_date_str)
    ].copy()

    for _, row in eligible_df.iterrows():
        zip_path = Path(row["landing_path"])
        if zip_path.exists():
            zip_path.unlink()
        purged_updates.append({
            "file_name": row["file_name"],
            "status": "PURGED_FROM_LANDING",
            "purged_at_utc": now_iso,
        })

    # Remove empty date directories
    if LANDING_BASE_DIR.exists():
        for feed_dir in LANDING_BASE_DIR.iterdir():
            if feed_dir.is_dir():
                for date_dir in feed_dir.iterdir():
                    if date_dir.is_dir() and date_dir.name < cutoff_date_str:
                        if not any(date_dir.iterdir()):
                            shutil.rmtree(date_dir, ignore_errors=True)
                            folders_removed += 1

    if purged_updates:
        df_purged = pd.DataFrame(purged_updates)
        (
            dt_log.merge(
                source=pa.Table.from_pandas(df_purged, preserve_index=False),
                predicate="target.file_name = source.file_name",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update({
                "status": "source.status",
                "purged_at_utc": "source.purged_at_utc",
            })
            .execute()
        )

print(f"Purged {len(purged_updates)} old ZIP file(s) and {folders_removed} empty date folder(s).")

# MARKDOWN ********************

# ### Cell 6: Inspect Final `meta_file_control_log` & `bronze_scada` Summary

# CELL ********************

if DeltaTable.is_deltatable(CONTROL_LOG_TABLE_PATH):
    df_final_log = DeltaTable(CONTROL_LOG_TABLE_PATH).to_pandas()
    print("Control Log Status Counts:")
    display(df_final_log["status"].value_counts().reset_index())
    display(df_final_log.sort_values("downloaded_at_utc", ascending=False).head(10))

result = {
    "status": "OK",
    "step_2_bronze": {"files_ingested": len(ingested_updates), "rows_upserted": total_rows_upserted},
    "step_3_cleanup": {"files_purged": len(purged_updates), "folders_removed": folders_removed},
    "timestamp_utc": now_iso,
}

try:
    import notebookutils
    notebookutils.notebook.exit(json.dumps(result))
except ImportError:
    pass
