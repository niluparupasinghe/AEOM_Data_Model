# Fabric notebook source


# MARKDOWN ********************

# # 00 - Master Orchestrator (Option B)
# Run this notebook from your **5-minute Microsoft Fabric Data Pipeline**.
# It executes `01_download_to_landing` and `02_unzip_to_bronze_and_cleanup` sequentially on the **same Python compute session** using `notebookutils.notebook.run()`.

# CELL ********************

import json
from datetime import datetime, timezone

def is_fabric_environment() -> bool:
    try:
        import notebookutils  # noqa: F401
        return True
    except ImportError:
        return False

IN_FABRIC = is_fabric_environment()
print(f"Running inside Microsoft Fabric: {IN_FABRIC}")

# MARKDOWN ********************

# ### Cell 2: Run Step 1 (`01_download_to_landing`)
# Wrapped in `try...except` so that even if AEMO's website is temporarily down, Step 2 will still process any previously downloaded ZIP files.

# CELL ********************

summary = {
    "orchestrator_start_utc": datetime.now(timezone.utc).isoformat(),
    "step_1_download": None,
    "step_2_bronze_and_cleanup": None,
}

try:
    if IN_FABRIC:
        import notebookutils
        print(">>> Starting 01_download_to_landing on current session...")
        res_1 = notebookutils.notebook.run(
            "01_download_to_landing",
            timeout_seconds=180,
            arguments={"retention_days": "7"}
        )
        summary["step_1_download"] = json.loads(res_1) if res_1 else {"status": "OK"}
    else:
        import importlib
        step1 = importlib.import_module("01_download_to_landing")
        summary["step_1_download"] = step1.main()
except Exception as e:
    print(f"[WARNING] Step 1 (Download) encountered an error: {e}")
    summary["step_1_download"] = {"status": "ERROR", "error": str(e)}

print("Step 1 Result:")
print(json.dumps(summary["step_1_download"], indent=2))

# MARKDOWN ********************

# ### Cell 3: Run Step 2 & 3 (`02_unzip_to_bronze_and_cleanup`)

# CELL ********************

if IN_FABRIC:
    import notebookutils
    print(">>> Starting 02_unzip_to_bronze_and_cleanup on current session...")
    res_2 = notebookutils.notebook.run(
        "02_unzip_to_bronze_and_cleanup",
        timeout_seconds=240,
        arguments={"retention_days": "7"}
    )
    summary["step_2_bronze_and_cleanup"] = json.loads(res_2) if res_2 else {"status": "OK"}
    notebookutils.notebook.exit(json.dumps(summary))
else:
    import importlib
    step2 = importlib.import_module("02_unzip_to_bronze_and_cleanup")
    summary["step_2_bronze_and_cleanup"] = step2.main(retention_days=7)

print("Full Orchestration Summary:")
print(json.dumps(summary, indent=2))
