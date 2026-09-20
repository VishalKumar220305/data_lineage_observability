"""
_inject_helpers.py

Shared helpers for the failure-injection scripts. Each scenario logs a REAL
pipeline_runs entry -- just like a normal job would -- so the dashboard's
"Recent pipeline runs" table shows exactly which run introduced the
problem, and freshness/MTTD calculations (which read finished_at from
pipeline_runs) work correctly off real data.

Two helpers, for two different needs:
  - log_backdated_run(): logs a new run at (approximately) the current
    time. Used by scenarios where the bug is introduced "right now"
    (schema drift, referential break) -- MTTD will accurately reflect
    near-immediate detection.
  - backdate_latest_run(): updates an EXISTING run's finished_at further
    into the past. Used ONLY by the stale-data scenario, where faking
    elapsed time is the entire point and is the only mechanically
    reliable way to do it (see its docstring for why).

Every run touched here is clearly labeled in its own error_message field
with a "[FAILURE INJECTION DEMO]" tag -- this is never hidden or disguised
as undisclosed real history. This mirrors standard, respected practice
(chaos engineering / fault injection testing).
"""

import os
from datetime import datetime, timedelta

import mysql.connector
from dotenv import load_dotenv

load_dotenv()


def _get_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 3306)),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
    )


def log_backdated_run(job_name: str, scenario_label: str,
                       rows_read: int = None, rows_written: int = None) -> int:
    """
    Inserts a pipeline_runs row at (approximately) the current time, labeled
    as a failure-injection demo run. Used by scenarios where the bug is
    "introduced right now" (schema drift, referential break) -- the
    resulting MTTD will accurately reflect near-immediate detection, which
    is a real, defensible result, not a faked one.
    """
    finished_at = datetime.now()
    started_at = finished_at - timedelta(seconds=20)

    conn = _get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO pipeline_runs
            (job_name, started_at, finished_at, status, rows_read, rows_written,
             duration_seconds, error_message)
        VALUES (%s, %s, %s, 'success', %s, %s, 20.0, %s)
        """,
        (job_name, started_at, finished_at, rows_read, rows_written,
         f"[FAILURE INJECTION DEMO] {scenario_label}"),
    )
    conn.commit()
    run_id = cursor.lastrowid
    cursor.close()
    conn.close()

    print(f"[inject] Logged run_id={run_id} for job='{job_name}' at current time.")
    print(f"[inject] Labeled in pipeline_runs.error_message: "
          f"'[FAILURE INJECTION DEMO] {scenario_label}'")
    return run_id


def backdate_latest_run(job_name: str, hours_ago: float, scenario_label: str) -> int:
    """
    Used ONLY by the stale-data scenario, where faking elapsed time is the
    entire point. Rather than inserting a new row (which the freshness
    lookup's "most recent" logic could ignore if a genuinely newer real run
    already exists), this directly updates the EXISTING most recent
    successful run for job_name, pushing its finished_at back by
    `hours_ago`. This is the only mechanically reliable way to simulate
    staleness without literally waiting -- and it's clearly labeled in that
    row's own error_message so it's never mistaken for undisclosed real
    history tampering.

    Raises if there's no existing successful run for this job to backdate.
    """
    conn = _get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT run_id FROM pipeline_runs
        WHERE job_name = %s AND status = 'success'
        ORDER BY finished_at DESC LIMIT 1
        """,
        (job_name,),
    )
    row = cursor.fetchone()
    if row is None:
        cursor.close()
        conn.close()
        raise RuntimeError(f"No existing successful run found for job '{job_name}' "
                            f"to backdate -- run the real pipeline job first.")

    run_id = row[0]
    new_finished_at = datetime.now() - timedelta(hours=hours_ago)

    cursor.execute(
        """
        UPDATE pipeline_runs
        SET finished_at = %s,
            error_message = %s
        WHERE run_id = %s
        """,
        (new_finished_at, f"[FAILURE INJECTION DEMO] {scenario_label}", run_id),
    )
    conn.commit()
    cursor.close()
    conn.close()

    print(f"[inject] Backdated existing run_id={run_id} for job='{job_name}': "
          f"finished_at moved to {new_finished_at.isoformat()} ({hours_ago:.0f}h ago).")
    print(f"[inject] Labeled in pipeline_runs.error_message: "
          f"'[FAILURE INJECTION DEMO] {scenario_label}'")
    return run_id
