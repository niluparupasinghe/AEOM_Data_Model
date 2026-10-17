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

# # 03 - Push AEMO ZIPs from Microsoft Fabric OneLake to Databricks Unity Catalog Volume
# **Where to run this notebook:** Import and run this notebook inside **Microsoft Fabric** (after `01_download_to_landing`)
# 
# Because **Databricks Free Edition** blocks outbound DNS to `nemweb.com.au` (`NameResolutionError`), this notebook takes the real AEMO `.zip` files that Microsoft Fabric already downloaded to `/lakehouse/default/Files/landing/` and pushes them directly into your **AWS Databricks Unity Catalog Volume** (`/Volumes/workspace/bronze/landing/_manual_upload/`) using the Databricks Files REST API (`PUT /api/2.0/fs/files/...`).

# MARKDOWN ********************

# ### Cell 1: Configure Databricks Workspace URL & Personal Access Token (PAT)
# Generate a PAT in your Databricks Free Edition workspace under **Settings -> Developer -> Access tokens -> Generate new token**.

# PARAMETERS CELL ********************

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

# Replace with your AWS Databricks Free Edition workspace URL and PAT token
DATABRICKS_HOST = os.getenv(
    "DATABRICKS_HOST",
    "https://dbc-957a6ecf-1636.cloud.databricks.com",
).rstrip("/")

DATABRICKS_TOKEN = os.getenv(
    "DATABRICKS_TOKEN",
    "dapi3fea19faeb0aaba31ff2da60f1c076fe",
)

# Optional: Enter your Databricks Workflow Job ID to auto-trigger the Databricks job when new files arrive
#DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID", "")

CATALOG = "aemo"
SCHEMA = "bronze"
VOLUME_NAME = "landing"
DATABRICKS_UPLOAD_SUBDIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/_manual_upload"

FABRIC_LANDING_DIR = Path("/lakehouse/default/Files/landing")
PUSH_STATE_FILE = FABRIC_LANDING_DIR / "_databricks_push_state.json"

print(f"Source (Fabric OneLake)      : {FABRIC_LANDING_DIR}")
print(f"Target (Databricks Volume)   : {DATABRICKS_HOST}{DATABRICKS_UPLOAD_SUBDIR}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Cell 2: Identify New `.zip` Files in Fabric OneLake That Have Not Been Pushed Yet

# CELL ********************

def load_pushed_files() -> set[str]:
    if PUSH_STATE_FILE.exists():
        try:
            data = json.loads(PUSH_STATE_FILE.read_text(encoding="utf-8"))
            return set(data.get("pushed_files", []))
        except Exception:
            pass
    return set()

def list_existing_databricks_upload_files(session: requests.Session) -> set[str]:
    url = f"{DATABRICKS_HOST}/api/2.0/fs/directories{DATABRICKS_UPLOAD_SUBDIR}"
    resp = session.get(url, timeout=30)
    if resp.status_code == 404:
        return set()
    resp.raise_for_status()
    contents = resp.json().get("contents", [])
    return {
        Path(item["path"]).name
        for item in contents
        if not item.get("is_directory", False)
    }

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "User-Agent": "PacificBlue-Fabric-To-Databricks-Bridge/1.0",
})

known_pushed = load_pushed_files()
remote_files = list_existing_databricks_upload_files(session)
already_handled = known_pushed | remote_files

all_local_zips = sorted(FABRIC_LANDING_DIR.rglob("*.zip")) if FABRIC_LANDING_DIR.exists() else []
candidate_zips = [p for p in all_local_zips if p.name not in already_handled]

print(f"Total ZIPs in Fabric Landing : {len(all_local_zips)}")
print(f"Already pushed to Databricks : {len(already_handled)}")
print(f"New ZIPs to push now         : {len(candidate_zips)}")
for p in candidate_zips[:15]:
    print(f"  [NEW] {p.name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Cell 3: Push New `.zip` Files to Databricks Unity Catalog Volume via Files REST API (`PUT /api/2.0/fs/files/...`)

# CELL ********************

def save_pushed_files(pushed_files: set[str]) -> None:
    PUSH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_pushed": len(pushed_files),
        "pushed_files": sorted(pushed_files),
    }
    PUSH_STATE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")

pushed_count = 0
total_bytes = 0

for zip_path in candidate_zips:
    target_api_url = (
        f"{DATABRICKS_HOST}/api/2.0/fs/files"
        f"{DATABRICKS_UPLOAD_SUBDIR}/{zip_path.name}?overwrite=true"
    )
    file_bytes = zip_path.read_bytes()
    resp = session.put(
        target_api_url,
        data=file_bytes,
        headers={"Content-Type": "application/octet-stream"},
        timeout=60,
    )
    resp.raise_for_status()

    pushed_count += 1
    total_bytes += len(file_bytes)
    known_pushed.add(zip_path.name)
    print(f"Pushed: {zip_path.name} ({len(file_bytes):,} bytes) -> {DATABRICKS_UPLOAD_SUBDIR}/{zip_path.name}")

if pushed_count > 0:
    save_pushed_files(known_pushed)
    print(f"\nSuccessfully pushed {pushed_count} ZIP file(s) ({total_bytes:,} bytes) to Databricks!")
else:
    print("No new ZIP files needed to be pushed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Cell 4: (Optional) Trigger Databricks Multi-Task Workflow Job & Exit

# CELL ********************

# job_run_info = None
# if pushed_count > 0 and DATABRICKS_JOB_ID and str(DATABRICKS_JOB_ID).strip().isdigit():
#     run_url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
#     r = session.post(run_url, json={"job_id": int(DATABRICKS_JOB_ID)}, timeout=30)
#     r.raise_for_status()
#     job_run_info = r.json()
#     print(f"Triggered Databricks Job {DATABRICKS_JOB_ID}: {job_run_info}")

# result = {
#     "status": "OK",
#     "pushed_files": pushed_count,
#     "skipped_already_pushed": len(all_local_zips) - pushed_count,
#     "total_bytes_pushed": total_bytes,
#     "databricks_target_volume": DATABRICKS_UPLOAD_SUBDIR,
#     "databricks_job_run": job_run_info,
#     "timestamp_utc": datetime.now(timezone.utc).isoformat(),
# }

# try:
#     import notebookutils
#     notebookutils.notebook.exit(json.dumps(result))
# except Exception:
#     pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
