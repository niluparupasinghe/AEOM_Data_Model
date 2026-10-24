# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

"""
00_master_orchestrator.py
=========================
Microsoft Fabric - Option B Parent Orchestrator Notebook (Pure Python)

Purpose:
- Attached as the single Notebook activity in a 5-minute Fabric Data Pipeline.
- Runs `01_download_to_landing` and `02_unzip_to_bronze_and_cleanup` sequentially
  on the SAME Python compute node using `notebookutils.notebook.run()`.
- Avoids spinning up two separate compute instances while keeping the Download
  and Bronze Ingestion code cleanly separated in two notebooks.
"""

import json
from datetime import datetime, timezone

def is_fabric_environment() -> bool:
  """Detect whether we are running inside Microsoft Fabric."""
  try:
      import notebookutils  # noqa: F401
      return True
  except ImportError:
      return False

is_fabric_environment()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def run_pipeline():
    run_started_utc = datetime.now(timezone.utc).isoformat()
    summary = {
        "orchestrator_start_utc": run_started_utc,
        "step_1_download": None,
        "step_2_bronze_and_cleanup": None,
    }

    if is_fabric_environment():
        import notebookutils

        # ---------------------------------------------------------------------
        # Step 1: Download new 5-minute ZIP files to Landing Zone
        # Wrapped in try/except so Step 2 still processes any previously
        # downloaded ZIP files even if AEMO's website is temporarily unreachable.
        # ---------------------------------------------------------------------
        try:
            print(">>> Starting 01_download_to_landing on current session...")
            res_1 = notebookutils.notebook.run(
                "01_download_to_landing",
                timeout_seconds=180,
                arguments={"retention_days": "7"}
            )
            summary["step_1_download"] = json.loads(res_1) if res_1 else {"status": "OK"}
        except Exception as e:
            print(f"[WARNING] Step 1 (Download) encountered an error: {e}")
            summary["step_1_download"] = {"status": "ERROR", "error": str(e)}

        # ---------------------------------------------------------------------
        # Step 2 & 3: Unzip Landing ZIPs -> Push to Bronze Table -> 7-Day Purge
        # ---------------------------------------------------------------------
        print(">>> Starting 02_unzip_to_bronze_and_cleanup on current session...")
        res_2 = notebookutils.notebook.run(
            "02_unzip_to_bronze_and_cleanup",
            timeout_seconds=240,
            arguments={"retention_days": "7"}
        )
        summary["step_2_bronze_and_cleanup"] = json.loads(res_2) if res_2 else {"status": "OK"}

        # Return structured JSON back to the Fabric Data Pipeline activity output
        notebookutils.notebook.exit(json.dumps(summary))

    else:
        # Local execution fallback for testing outside Fabric
        import importlib
        step1 = importlib.import_module("01_download_to_landing")
        step2 = importlib.import_module("02_unzip_to_bronze_and_cleanup")

        try:
            print(">>> [Local Mode] Running Step 1: 01_download_to_landing...")
            summary["step_1_download"] = step1.main()
        except Exception as e:
            print(f"[WARNING] Step 1 failed: {e}")
            summary["step_1_download"] = {"status": "ERROR", "error": str(e)}

        print(">>> [Local Mode] Running Step 2 & 3: 02_unzip_to_bronze_and_cleanup...")
        summary["step_2_bronze_and_cleanup"] = step2.main(retention_days=7)
        print("\nOrchestration Complete:", json.dumps(summary, indent=2))
        return summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if __name__ == "__main__":
    run_pipeline()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
