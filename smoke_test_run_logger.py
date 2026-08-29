"""
smoke_test_run_logger.py

Quick sanity check for Day 1: confirms run_logger.py can connect to MySQL
and successfully write a start + finish record into pipeline_runs.

Run this from the project root (same folder that has .env in it):
    python smoke_test_run_logger.py
"""

from pipeline.instrumentation.run_logger import RunLogger

logger = RunLogger(job_name="smoke_test")

run_id = logger.start()
print(f"Started test run with run_id={run_id}")

# Pretend some work happened here (in Day 2 this will be real read/write counts)
logger.finish(status="success", rows_read=10, rows_written=10)

print("\nDone. Now check MySQL Workbench:")
print("  Run: SELECT * FROM pipeline_runs;")
print("  You should see one row with job_name='smoke_test', status='success'.")
