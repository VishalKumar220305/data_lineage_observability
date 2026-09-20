"""
mttd.py

Computes "detection latency" for a failing check: the time between when the
table was last WRITTEN (the finished_at of the job that produced it) and
when the FAILING check that caught the problem actually ran.

This doesn't need any new schema -- it's entirely derived from timestamps
we already have in pipeline_runs and dq_check_results. Day 6's failure
injection scenarios will use this to report a real, measured MTTD number
for each scenario, rather than an estimated one.
"""


def compute_detection_latency(conn, table_name: str, check_checked_at) -> dict:
    """
    Returns:
        {
            "table_name": ...,
            "last_written_at": datetime or None,
            "detected_at": check_checked_at,
            "latency_seconds": float or None,
            "latency_human": "..." or "unknown -- no write recorded",
        }
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT job_name FROM lineage_edges
        WHERE target_table = %s
        ORDER BY detected_at DESC LIMIT 1
        """,
        (table_name,),
    )
    row = cursor.fetchone()
    source_job = row[0] if row else None

    last_written_at = None
    if source_job:
        cursor.execute(
            """
            SELECT finished_at FROM pipeline_runs
            WHERE job_name = %s AND status = 'success' AND finished_at <= %s
            ORDER BY finished_at DESC LIMIT 1
            """,
            (source_job, check_checked_at),
        )
        write_row = cursor.fetchone()
        last_written_at = write_row[0] if write_row else None

    cursor.close()

    if last_written_at is None:
        return {
            "table_name": table_name,
            "last_written_at": None,
            "detected_at": check_checked_at,
            "latency_seconds": None,
            "latency_human": "unknown -- no prior successful write found for this table",
        }

    latency_seconds = (check_checked_at - last_written_at).total_seconds()

    if latency_seconds < 60:
        human = f"{latency_seconds:.0f} seconds"
    elif latency_seconds < 3600:
        human = f"{latency_seconds / 60:.1f} minutes"
    else:
        human = f"{latency_seconds / 3600:.2f} hours"

    return {
        "table_name": table_name,
        "last_written_at": last_written_at,
        "detected_at": check_checked_at,
        "latency_seconds": latency_seconds,
        "latency_human": human,
    }
