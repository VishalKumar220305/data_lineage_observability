"""
dq_runner.py

Orchestrates every configured DQ check across every table listed in
config.yaml's data_quality.tables section, and writes each result into
dq_check_results, tied to a single run_id representing this DQ suite
execution.

Usage:
    python -m data_quality.dq_runner
"""

import os
import json
from datetime import datetime

import yaml
import mysql.connector
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from pipeline.instrumentation.run_logger import RunLogger
from data_quality.checks.null_checks import check_nulls
from data_quality.checks.duplicate_checks import check_duplicates
from data_quality.checks.referential_integrity import check_referential_integrity
from data_quality.checks.schema_drift import check_schema_drift
from data_quality.checks.freshness_checks import check_freshness
from data_quality.mttd import compute_detection_latency
from alerting.notifier import send_failure_alert
from lineage.graph.lineage_graph import build_graph_from_db
from lineage.graph.blast_radius import compute_blast_radius

load_dotenv()

JOB_NAME = "dq_check_run"


def _get_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 3306)),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
    )


def _load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)


def _write_result(conn, run_id: int, table_name: str, result: dict):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO dq_check_results (run_id, table_name, check_type, column_name, status, detail)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (run_id, table_name, result["check_type"], result.get("column_name"),
         result["status"], result.get("detail")),
    )
    conn.commit()
    cursor.close()


def _get_last_schema_snapshot(conn, table_name: str):
    """Fetches the current_schema JSON embedded in the most recent
    schema_drift detail for this table, or None if there isn't one yet."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT detail FROM dq_check_results
        WHERE table_name = %s AND check_type = 'schema_drift'
        ORDER BY checked_at DESC LIMIT 1
        """,
        (table_name,),
    )
    row = cursor.fetchone()
    cursor.close()
    if row is None:
        return None
    detail = row[0]
    marker = "current_schema: "
    idx = detail.find(marker)
    if idx == -1:
        # first-run detail message doesn't have the marker in the same spot;
        # handle the "no prior snapshot -- recording baseline: {...}" case
        marker2 = "recording baseline: "
        idx2 = detail.find(marker2)
        if idx2 == -1:
            return None
        return detail[idx2 + len(marker2):]
    return detail[idx + len(marker):]


def _get_last_successful_write(conn, job_name: str):
    """Returns the finished_at datetime of the most recent successful
    pipeline_runs row for this job, or None if there isn't one."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT finished_at FROM pipeline_runs
        WHERE job_name = %s AND status = 'success'
        ORDER BY finished_at DESC LIMIT 1
        """,
        (job_name,),
    )
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row else None


def _get_job_for_table(conn, table_name: str):
    """Looks up which job produces this table via lineage_edges."""
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
    cursor.close()
    return row[0] if row else None


def run():
    config = _load_config()
    dq_config = config["data_quality"]

    spark = SparkSession.builder \
        .appName(JOB_NAME) \
        .enableHiveSupport() \
        .getOrCreate()

    conn = _get_connection()
    logger = RunLogger(job_name=JOB_NAME)
    run_id = logger.start()

    total_checks = 0
    total_failed = 0
    failing_checks_for_alert = []

    try:
        for table_config in dq_config["tables"]:
            table_name = table_config["name"]
            print(f"\n--- Checking {table_name} ---")

            df = spark.read.table(table_name)
            results = []

            # --- Null checks ---
            null_results = check_nulls(
                df, table_config.get("null_check_columns", []),
                dq_config["null_checks"]["max_null_rate_pct"],
            )
            results.extend(null_results)

            # --- Duplicate check ---
            if table_config.get("key_columns"):
                dup_result = check_duplicates(
                    df, table_config["key_columns"],
                    dq_config["duplicate_checks"]["max_duplicate_rate_pct"],
                )
                results.append(dup_result)

            # --- Referential integrity checks ---
            for fk_def in table_config.get("referential_integrity", []):
                parent_df = spark.read.table(fk_def["parent_table"])
                ri_result = check_referential_integrity(
                    df, fk_def["column"], parent_df, fk_def["parent_column"],
                    dq_config["referential_integrity"]["fail_on_any_orphan"],
                )
                results.append(ri_result)

            # --- Schema drift check ---
            current_schema = {f.name: str(f.dataType) for f in df.schema.fields}
            previous_schema_json = _get_last_schema_snapshot(conn, table_name)
            drift_result = check_schema_drift(
                current_schema, previous_schema_json,
                dq_config["schema_drift"]["fail_on_column_removed"],
                dq_config["schema_drift"]["fail_on_type_changed"],
                dq_config["schema_drift"]["warn_on_column_added"],
            )
            results.append(drift_result)

            # --- Freshness check ---
            if table_config.get("freshness_check"):
                source_job = _get_job_for_table(conn, table_name)
                last_write = _get_last_successful_write(conn, source_job) if source_job else None
                freshness_result = check_freshness(
                    last_write, dq_config["freshness_checks"]["max_staleness_hours"],
                )
                results.append(freshness_result)

            # --- Write all results for this table ---
            for result in results:
                checked_at = datetime.now()
                _write_result(conn, run_id, table_name, result)
                total_checks += 1
                marker = "FAIL" if result["status"] == "fail" else \
                          "WARN" if result["status"] == "warn" else "pass"
                if result["status"] == "fail":
                    total_failed += 1
                    failing_checks_for_alert.append({
                        "table_name": table_name,
                        "check_type": result["check_type"],
                        "column_name": result.get("column_name"),
                        "detail": result.get("detail"),
                        "checked_at": checked_at,
                    })
                col_label = f"[{result['column_name']}] " if result.get("column_name") else ""
                print(f"  {marker:4} {result['check_type']:22} {col_label}{result['detail'][:80]}")

        # --- Blast radius + MTTD for every failure, then one alert email ---
        if failing_checks_for_alert:
            print(f"\n{'=' * 70}")
            print("Computing blast radius and detection latency for each failure...")
            print(f"{'=' * 70}")

            graph = build_graph_from_db(conn)
            for check in failing_checks_for_alert:
                blast = compute_blast_radius(graph, check["table_name"])
                check["blast_radius"] = blast

                latency = compute_detection_latency(conn, check["table_name"], check["checked_at"])
                check["mttd"] = latency

                print(f"\n[{check['table_name']}] {check['check_type']}")
                print(f"    Blast radius: {blast['affected_count']} table(s) -- "
                      f"{blast['affected_tables'] or 'none'}")
                print(f"    Detection latency: {latency['latency_human']}")

            send_failure_alert(failing_checks_for_alert)

        logger.finish(status="success", rows_read=total_checks, rows_written=total_checks)

    except Exception as e:
        logger.finish(status="failed", error_message=str(e))
        raise
    finally:
        conn.close()
        spark.stop()

    print(f"\n{'=' * 70}")
    print(f"DQ run complete. {total_checks} checks run, {total_failed} failed.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    run()
