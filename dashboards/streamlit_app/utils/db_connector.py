"""
db_connector.py

Shared helpers every Streamlit page uses to talk to the metadata store and
build the lineage graph. Kept separate from the pages themselves so the
same logic isn't duplicated across app.py and the 3 pages.
"""

import os
import streamlit as st
import mysql.connector
from dotenv import load_dotenv

from lineage.graph.lineage_graph import build_graph_from_db

load_dotenv()


def get_connection():
    """A fresh connection per call -- Streamlit reruns the whole script on
    every interaction, so we don't try to hold one open across reruns."""
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 3306)),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
    )


@st.cache_data(ttl=30)
def get_lineage_graph_edges():
    """Returns the raw edge list (cached 30s so navigating between pages
    doesn't re-query MySQL every time). Returned as plain data, not a
    networkx object, since Streamlit's cache needs picklable results."""
    conn = get_connection()
    graph = build_graph_from_db(conn)
    conn.close()
    return list(graph.edges(data=True)), list(graph.nodes())


@st.cache_data(ttl=30)
def get_latest_dq_status_per_table():
    """Returns {table_name: 'pass'|'warn'|'fail'} using the WORST status
    across that table's most recent check of each check_type. This is what
    colors nodes in the lineage graph page."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT r1.table_name, r1.status
        FROM dq_check_results r1
        INNER JOIN (
            SELECT table_name, check_type, MAX(checked_at) AS max_checked_at
            FROM dq_check_results
            GROUP BY table_name, check_type
        ) latest
        ON r1.table_name = latest.table_name
           AND r1.check_type = latest.check_type
           AND r1.checked_at = latest.max_checked_at
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    severity = {"pass": 0, "warn": 1, "fail": 2}
    worst_status = {}
    for row in rows:
        table, status = row["table_name"], row["status"]
        if table not in worst_status or severity[status] > severity[worst_status[table]]:
            worst_status[table] = status
    return worst_status


@st.cache_data(ttl=30)
def get_dq_summary():
    """Overall pass/warn/fail counts across the most recent full DQ run."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT status, COUNT(*) as count
        FROM dq_check_results r1
        INNER JOIN (
            SELECT table_name, check_type, MAX(checked_at) AS max_checked_at
            FROM dq_check_results
            GROUP BY table_name, check_type
        ) latest
        ON r1.table_name = latest.table_name
           AND r1.check_type = latest.check_type
           AND r1.checked_at = latest.max_checked_at
        GROUP BY status
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {row["status"]: row["count"] for row in rows}


@st.cache_data(ttl=30)
def get_all_latest_dq_results():
    """Every table's most recent result for every check_type -- the full
    detail table the DQ Overview page displays."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT r1.table_name, r1.check_type, r1.column_name, r1.status,
               r1.detail, r1.checked_at
        FROM dq_check_results r1
        INNER JOIN (
            SELECT table_name, check_type, MAX(checked_at) AS max_checked_at
            FROM dq_check_results
            GROUP BY table_name, check_type
        ) latest
        ON r1.table_name = latest.table_name
           AND r1.check_type = latest.check_type
           AND r1.checked_at = latest.max_checked_at
        ORDER BY r1.table_name, r1.check_type
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


@st.cache_data(ttl=30)
def get_dq_history(days=7):
    """Pass/warn/fail counts grouped by day, for the trend chart."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT DATE(checked_at) as check_date, status, COUNT(*) as count
        FROM dq_check_results
        WHERE checked_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
        GROUP BY DATE(checked_at), status
        ORDER BY check_date
    """, (days,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


@st.cache_data(ttl=30)
def get_recent_pipeline_runs(limit=20):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT run_id, job_name, started_at, finished_at, status,
               rows_read, rows_written, duration_seconds
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT %s
    """, (limit,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows
