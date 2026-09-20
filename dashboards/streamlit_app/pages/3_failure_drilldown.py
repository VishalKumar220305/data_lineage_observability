"""
3_failure_drilldown.py

Lists every currently-failing DQ check and, for each, shows the full
downstream blast radius -- every table that could be affected by that
failure, not just the table where the check failed. This is the page
Day 6's failure-injection scenarios will demonstrate live.
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))

import streamlit as st
import networkx as nx

from dashboards.streamlit_app.utils.db_connector import get_connection, get_lineage_graph_edges
from lineage.graph.blast_radius import get_failing_tables_with_blast_radius

st.set_page_config(page_title="Failure Drilldown", layout="wide")
st.title("Failure Drilldown")
st.caption("Every currently-failing check, and everything downstream it could be affecting.")

# Rebuild the graph (small enough that re-fetching per page load is fine)
edges_raw, nodes_raw = get_lineage_graph_edges()
graph = nx.DiGraph()
for source, target, attrs in edges_raw:
    graph.add_edge(source, target, **attrs)
for node in nodes_raw:
    graph.add_node(node)

conn = get_connection()
failing = get_failing_tables_with_blast_radius(conn, graph)
conn.close()

if not failing:
    st.success("No failing checks right now — nothing to drill into. "
               "🎉")
    st.stop()

st.warning(f"{len(failing)} failing check(s) found.")

for item in failing:
    blast = item["blast_radius"]
    with st.expander(
        f"🔴 {item['table_name']} — {item['check_type']}"
        + (f" [{item['column_name']}]" if item.get("column_name") else ""),
        expanded=True,
    ):
        col1, col2 = st.columns([1, 1])

        with col1:
            st.markdown("**Failure detail**")
            st.code(item["detail"], language=None)
            st.caption(f"Checked at: {item['checked_at']}")

        with col2:
            st.markdown(f"**Blast radius: {blast['affected_count']} table(s) potentially affected**")
            if blast["affected_tables"]:
                for affected_table in blast["affected_tables"]:
                    path = blast["paths"].get(affected_table, [])
                    path_str = " → ".join(path) if path else affected_table
                    st.write(f"- **{affected_table}**")
                    st.caption(f"  path: {path_str}")
            else:
                st.write("_(nothing downstream — this failure is contained "
                          "to this table)_")
