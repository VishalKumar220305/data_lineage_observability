"""
run_logger.py

Wraps a pipeline job with logging into the pipeline_runs table.
Every PySpark job in pipeline/jobs/ will use this so that every run is
tracked automatically, without each job having to write its own logging code.

Usage pattern (used in Day 2 when the actual jobs are written):

    from pipeline.instrumentation.run_logger import RunLogger

    logger = RunLogger(job_name="load_customers")
    logger.start()
    try:
        # ... read data, transform, write ...
        logger.finish(status="success", rows_read=1000, rows_written=980)
    except Exception as e:
        logger.finish(status="failed", error_message=str(e))
        raise
"""

import os
import time
from datetime import datetime
import mysql.connector
from dotenv import load_dotenv

load_dotenv()


def _get_connection():
    """Opens a connection to the metadata store using values from .env."""
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 3306)),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
    )


class RunLogger:
    """
    Tracks a single pipeline job execution from start to finish and writes
    the result into the pipeline_runs table.
    """

    def __init__(self, job_name: str):
        self.job_name = job_name
        self.run_id = None
        self._start_time = None

    def start(self):
        """Call this right before the job's actual work begins."""
        self._start_time = time.time()
        started_at = datetime.now()

        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO pipeline_runs (job_name, started_at, status)
            VALUES (%s, %s, 'running')
            """,
            (self.job_name, started_at),
        )
        conn.commit()
        self.run_id = cursor.lastrowid
        cursor.close()
        conn.close()

        print(f"[RunLogger] Started run_id={self.run_id} for job='{self.job_name}'")
        return self.run_id

    def finish(self, status: str, rows_read: int = None, rows_written: int = None,
               error_message: str = None):
        """
        Call this when the job finishes (success or failure).
        status must be 'success' or 'failed'.
        """
        if self.run_id is None:
            raise RuntimeError("finish() called before start() — no run_id to update.")

        duration_seconds = round(time.time() - self._start_time, 2)
        finished_at = datetime.now()

        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE pipeline_runs
            SET finished_at = %s,
                status = %s,
                rows_read = %s,
                rows_written = %s,
                duration_seconds = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (finished_at, status, rows_read, rows_written,
             duration_seconds, error_message, self.run_id),
        )
        conn.commit()
        cursor.close()
        conn.close()

        print(f"[RunLogger] Finished run_id={self.run_id} status={status} "
              f"duration={duration_seconds}s rows_read={rows_read} rows_written={rows_written}")
