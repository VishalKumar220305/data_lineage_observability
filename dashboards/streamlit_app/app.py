"""
app.py

Home page of the observability dashboard. Shows a high-level overview:
DQ pass/warn/fail counts, table count, and recent pipeline run history.
The detailed lineage graph, full DQ breakdown, and failure drill-down each
live on their own page (see pages/).

Run from the project root:
    streamlit run dashboards\\streamlit_app\\app.py
"""

import sys
from pathlib import Path

# Make sure the project root is importable regardless of where Streamlit
# is launched from -- same path issue we hit with the PySpark jobs.
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

import streamlit as st
from dashboards.streamlit_app.utils.db_connector import (
    get_dq_summary, get_recent_pipeline_runs, get_lineage_graph_edges,
)

st.set_page_config(page_title="Data Lineage & Observability", layout="wide")

st.title("Data Lineage & Observability Dashboard")
st.caption("Olist e-commerce pipeline — standalone portfolio project")

# --- Top-level metrics ---
col1, col2, col3, col4 = st.columns(4)

try:
    dq_summary = get_dq_summary()
    edges, nodes = get_lineage_graph_edges()

    with col1:
        st.metric("Tables tracked", len(nodes))
    with col2:
        st.metric("Lineage edges", len(edges))
    with col3:
        st.metric("Checks passing", dq_summary.get("pass", 0))
    with col4:
        fail_count = dq_summary.get("fail", 0)
        st.metric("Checks failing", fail_count,
                   delta=None if fail_count == 0 else "needs attention",
                   delta_color="inverse")

except Exception as e:
    st.error(f"Could not connect to the metadata store: {e}")
    st.info("Make sure your .env file is set up and MySQL is running, "
            "then refresh this page.")
    st.stop()

st.divider()

# --- Recent pipeline runs ---
st.subheader("Recent pipeline runs")
runs = get_recent_pipeline_runs(limit=15)

if runs:
    st.dataframe(
        runs,
        column_config={
            "run_id": "Run ID",
            "job_name": "Job",
            "started_at": "Started",
            "finished_at": "Finished",
            "status": "Status",
            "rows_read": "Rows read",
            "rows_written": "Rows written",
            "duration_seconds": "Duration (s)",
        },
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No pipeline runs recorded yet.")

st.divider()
st.caption("Use the sidebar to explore the lineage graph, full DQ breakdown, "
           "or drill into a specific failure.")
