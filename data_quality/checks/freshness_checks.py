"""
freshness_checks.py

Checks how long it's been since a table was last successfully written, using
the finished_at timestamp of the most recent successful pipeline_runs entry
for the job that produces this table (dq_runner.py resolves that job via
lineage_edges before calling this).

Pure comparison logic -- no database or Spark access here.
"""

from datetime import datetime


def check_freshness(last_successful_write: datetime, max_staleness_hours: float,
                     now: datetime = None) -> dict:
    """
    last_successful_write: datetime of the last successful write to this table,
        or None if there's no successful run on record at all.

    Returns a single result dict:
        {"check_type": "freshness", "column_name": None, "status": "pass"|"fail",
         "detail": "..."}
    """
    now = now or datetime.now()

    if last_successful_write is None:
        return {
            "check_type": "freshness",
            "column_name": None,
            "status": "fail",
            "detail": "no successful pipeline run found for this table's source job",
        }

    staleness_hours = (now - last_successful_write).total_seconds() / 3600
    status = "fail" if staleness_hours > max_staleness_hours else "pass"

    detail = (f"last_written={last_successful_write.isoformat()}, "
              f"staleness={staleness_hours:.2f}h, threshold={max_staleness_hours}h")

    return {
        "check_type": "freshness",
        "column_name": None,
        "status": status,
        "detail": detail,
    }
