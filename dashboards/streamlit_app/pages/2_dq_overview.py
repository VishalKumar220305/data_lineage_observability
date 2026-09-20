"""
2_dq_overview.py

Full breakdown of the most recent DQ check results across every table,
plus a trend of pass/warn/fail counts over the last several days.
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd

from dashboards.streamlit_app.utils.db_connector import get_all_latest_dq_results, get_dq_history

st.set_page_config(page_title="DQ Overview", layout="wide")
st.title("Data Quality Overview")

results = get_all_latest_dq_results()

if not results:
    st.warning("No DQ check results yet -- run the DQ suite (Day 3) first.")
    st.stop()

df = pd.DataFrame(results)

# --- Filters ---
col1, col2 = st.columns(2)
with col1:
    status_filter = st.multiselect(
        "Filter by status", options=["pass", "warn", "fail"],
        default=["pass", "warn", "fail"],
    )
with col2:
    table_filter = st.multiselect(
        "Filter by table", options=sorted(df["table_name"].unique()),
        default=sorted(df["table_name"].unique()),
    )

filtered = df[df["status"].isin(status_filter) & df["table_name"].isin(table_filter)]

# --- Summary counts ---
st.subheader("Summary (most recent check of each type, per table)")
summary_cols = st.columns(3)
for i, status in enumerate(["pass", "warn", "fail"]):
    count = (df["status"] == status).sum()
    summary_cols[i].metric(status.upper(), count)

st.divider()

# --- Detail table ---
st.subheader("Detail")


def _status_emoji(status):
    return {"pass": "🟢", "warn": "🟠", "fail": "🔴"}.get(status, "⚪")


display_df = filtered.copy()
display_df["status"] = display_df["status"].apply(lambda s: f"{_status_emoji(s)} {s}")

st.dataframe(
    display_df,
    column_config={
        "table_name": "Table",
        "check_type": "Check type",
        "column_name": "Column",
        "status": "Status",
        "detail": "Detail",
        "checked_at": "Checked at",
    },
    use_container_width=True,
    hide_index=True,
)

st.divider()

# --- Trend ---
st.subheader("Trend (last 7 days)")
history = get_dq_history(days=7)

if history:
    hist_df = pd.DataFrame(history)
    pivot = hist_df.pivot_table(
        index="check_date", columns="status", values="count", fill_value=0
    )
    st.bar_chart(pivot)
else:
    st.info("Not enough history yet to show a trend -- run the DQ suite "
            "across a few different days to build this up.")
