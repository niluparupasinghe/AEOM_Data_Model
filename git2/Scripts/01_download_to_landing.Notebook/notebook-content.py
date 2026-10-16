# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "343fbc4b-d150-4c4c-bf5e-753405b3421b",
# META       "default_lakehouse_name": "AEMO_LH1",
# META       "default_lakehouse_workspace_id": "4188c194-00fe-4472-b3de-0466415d8e4f",
# META       "known_lakehouses": [
# META         {
# META           "id": "343fbc4b-d150-4c4c-bf5e-753405b3421b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 01 - Download AEMO 5-Minute ZIPs to Landing Folder
# Run each cell interactively to see:
# 1. Which files are already recorded in `meta_file_control_log`
# 2. Which `.zip` files are currently live on AEMO NEMWEB
# 3. Which new `.zip` files get downloaded into `Files/landing/Dispatch_SCADA/YYYY-MM-DD/`
# 4. The updated `meta_file_control_log` Delta table

# MARKDOWN ********************

# ### Cell 1: Imports & Lakehouse Configuration

# CELL ********************

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import notebookutils
import pandas as pd
import requests
from deltalake import DeltaTable, write_deltalake

# AEMO NEM operates strictly on AEST (UTC+10) year-round (no Daylight Saving)
NEM_TZ = timezone(timedelta(hours=10))

# 1. Local FUSE mount for downloading/reading raw ZIP files in /Files
LAKEHOUSE_ROOT = Path("/lakehouse/default")
LANDING_BASE_DIR = LAKEHOUSE_ROOT / "Files" / "landing"

# 2. OneLake ABFSS URI + 'bronze' schema for Delta Tables in /Tables
ctx = notebookutils.runtime.context
ws_id = ctx["defaultLakehouseWorkspaceId"]
lh_id = ctx["defaultLakehouseId"]

TABLES_ABFS_BASE = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables"
CONTROL_LOG_TABLE_PATH = f"{TABLES_ABFS_BASE}/bronze/meta_file_control_log"

def get_storage_options() -> dict:
    return {
        "bearer_token": notebookutils.credentials.getToken("storage"),
        "use_fabric_endpoint": "true",
    }

NEMWEB_FEEDS = {
    "Dispatch_SCADA": {
        "url": "https://nemweb.com.au/Reports/Current/Dispatch_SCADA/",
        "pattern": r"(PUBLIC_DISPATCHSCADA_[0-9_]+\.zip)",
    }
}

print(f"Landing Folder (FUSE) : {LANDING_BASE_DIR}")
print(f"Control Log (ABFSS)   : {CONTROL_LOG_TABLE_PATH}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Cell 2: Check `meta_file_control_log` for Previously Processed Files
# Before downloading anything, we check our Delta control table so we never re-download the same file.

# CELL ********************

storage_opts = get_storage_options()

if DeltaTable.is_deltatable(CONTROL_LOG_TABLE_PATH, storage_options=storage_opts):
    dt_log = DeltaTable(CONTROL_LOG_TABLE_PATH, storage_options=storage_opts)
    df_existing_log = dt_log.to_pandas()
    known_files = set(df_existing_log["file_name"].dropna().unique())
    print(f"Found {len(known_files):,} tracked files in bronze.meta_file_control_log.")
    display(df_existing_log.sort_values("downloaded_at_utc", ascending=False).head(10))
else:
    known_files = set()
    print("bronze.meta_file_control_log does not exist yet (first run).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Cell 3: Poll AEMO NEMWEB Directory & Identify New Files
# Scrapes the live NEMWEB directory HTML and compares the latest `.zip` filenames against `known_files`.

# CELL ********************

session = requests.Session()
session.headers.update({"User-Agent": "PacificBlue-Fabric-Ingestion/1.0"})

files_to_download = []
skipped_count = 0

for feed_name, cfg in NEMWEB_FEEDS.items():
    print(f"Polling {feed_name}: {cfg['url']}")
    resp = session.get(cfg["url"], timeout=30)
    resp.raise_for_status()

    available_zips = sorted(set(re.findall(cfg["pattern"], resp.text)))
    candidate_zips = available_zips
    print(f"Total ZIPs listed on page: {len(available_zips)} | Checking {len(candidate_zips)} files...")

    for zf_name in candidate_zips:
        if zf_name in known_files:
            skipped_count += 1
        else:
            files_to_download.append((feed_name, cfg["url"], zf_name))

print(f"\nNew files to download : {len(files_to_download)}")
print(f"Already processed     : {skipped_count}")
for _, _, fname in files_to_download[:15]:
    print(f"  [NEW] {fname}")
if len(files_to_download) > 15:
    print(f"  ... and {len(files_to_download) - 15} more.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Cell 4: Download New ZIP Files into Date-Partitioned Landing Folder
# Saves each file under `/Files/landing/<feed_name>/<YYYY-MM-DD>/<file_name>.zip`.

# CELL ********************

def extract_settlement_date_str(zip_filename: str, fallback_dt: datetime) -> str:
    match = re.search(r"_(\d{4})(\d{2})(\d{2})\d{4}_", zip_filename)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return fallback_dt.strftime("%Y-%m-%d")

now_utc = datetime.now(timezone.utc)
now_nem = datetime.now(NEM_TZ)
new_log_records = []

for feed_name, base_url, zf_name in files_to_download:
    date_folder = extract_settlement_date_str(zf_name, now_nem)
    target_dir = LANDING_BASE_DIR / feed_name / date_folder
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / zf_name

    try:
        z_resp = session.get(base_url + zf_name, timeout=30)
    except Exception as e:
        print(f"Error downloading: {zf_name}) -> {e}")
    #z_resp.raise_for_status()

    with open(target_path, "wb") as f:
        f.write(z_resp.content)

    print(f"Downloaded: {zf_name} ({len(z_resp.content):,} bytes) -> {target_path}")

    new_log_records.append({
        "file_name": zf_name,
        "feed_name": feed_name,
        "landing_date_folder": date_folder,
        "landing_path": str(target_path),
        "file_size_bytes": len(z_resp.content),
        "status": "DOWNLOADED",
        "downloaded_at_utc": now_utc.isoformat(),
        "ingested_at_utc": None,
        "purged_at_utc": None,
        "bronze_row_count": 0,
    })
    #break

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Cell 5: Update `meta_file_control_log` & Inspect Results

# CELL ********************

if new_log_records:
    df_new = pd.DataFrame(new_log_records)
    df_new["ingested_at_utc"] = df_new["ingested_at_utc"].astype("string")
    df_new["purged_at_utc"] = df_new["purged_at_utc"].astype("string")

    write_deltalake(
        CONTROL_LOG_TABLE_PATH,
        df_new,
        mode="append",
        schema_mode="merge",
        storage_options=get_storage_options(),
    )
    print(f"Appended {len(df_new)} new record(s) to bronze.meta_file_control_log.")
    display(df_new)
else:
    print("No new files were downloaded in this run.")

result = {
    "status": "OK",
    "downloaded_files": len(new_log_records),
    "skipped_already_processed": skipped_count,
    "timestamp_utc": now_utc.isoformat(),
}

try:
    notebookutils.notebook.exit(json.dumps(result))
except Exception:
    pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Delete a schema

# CELL ********************

#notebookutils.fs.rm(f"{TABLES_ABFS_BASE}/meta_file_control_log", recurse=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
