"""
1_lineage_graph.py

Renders the full lineage graph as an interactive plotly figure, with nodes
colored by their latest DQ status (green=pass, orange=warn, red=fail).
Since click-to-select on plotly nodes needs an extra dependency we didn't
include in requirements.txt, drill-down works via a dropdown instead --
pick a table, see its upstream sources and downstream consumers listed
below the graph.
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))

import streamlit as st
import networkx as nx
import plotly.graph_objects as go

from dashboards.streamlit_app.utils.db_connector import (
    get_lineage_graph_edges, get_latest_dq_status_per_table,
)
from lineage.graph.lineage_graph import build_graph_from_edges, get_upstream, get_downstream

st.set_page_config(page_title="Lineage Graph", layout="wide")
st.title("Lineage Graph")

STATUS_COLORS = {"pass": "#2ecc71", "warn": "#f39c12", "fail": "#e74c3c", None: "#95a5a6"}

# --- Rebuild the graph from cached edge data ---
edges_raw, nodes_raw = get_lineage_graph_edges()
graph = nx.DiGraph()
for source, target, attrs in edges_raw:
    graph.add_edge(source, target, **attrs)
for node in nodes_raw:
    graph.add_node(node)

dq_status = get_latest_dq_status_per_table()

if graph.number_of_nodes() == 0:
    st.warning("No lineage data yet -- run the pipeline jobs and the lineage "
               "parser (Day 1-2) first.")
    st.stop()

# --- Layout ---
pos = nx.spring_layout(graph, seed=42, k=0.8)

# --- Build edge traces ---
edge_x, edge_y = [], []
for source, target in graph.edges():
    x0, y0 = pos[source]
    x1, y1 = pos[target]
    edge_x += [x0, x1, None]
    edge_y += [y0, y1, None]

edge_trace = go.Scatter(
    x=edge_x, y=edge_y, mode="lines",
    line=dict(width=1, color="#888"),
    hoverinfo="none",
)

# --- Build node trace ---
node_x, node_y, node_text, node_color = [], [], [], []
for node in graph.nodes():
    x, y = pos[node]
    node_x.append(x)
    node_y.append(y)
    status = dq_status.get(node)
    node_text.append(f"{node}<br>status: {status or 'not checked'}")
    node_color.append(STATUS_COLORS.get(status, STATUS_COLORS[None]))

node_trace = go.Scatter(
    x=node_x, y=node_y, mode="markers+text",
    text=[n for n in graph.nodes()],
    textposition="top center",
    hovertext=node_text,
    hoverinfo="text",
    marker=dict(size=18, color=node_color, line=dict(width=1, color="#333")),
)

fig = go.Figure(data=[edge_trace, node_trace])
fig.update_layout(
    showlegend=False,
    height=600,
    margin=dict(l=20, r=20, t=20, b=20),
    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
)

st.plotly_chart(fig, use_container_width=True)

st.caption("🟢 pass   🟠 warn   🔴 fail   ⚪ not yet checked")

st.divider()

# --- Drill-down ---
st.subheader("Drill down on a table")
selected_table = st.selectbox("Choose a table", sorted(graph.nodes()))

if selected_table:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"**Upstream of `{selected_table}`** (feeds into it)")
        upstream = get_upstream(graph, selected_table)
        if upstream:
            for t in sorted(upstream):
                st.write(f"- {t}")
        else:
            st.write("_(none — this is a root source)_")

    with col2:
        st.markdown(f"**Downstream of `{selected_table}`** (depends on it)")
        downstream = get_downstream(graph, selected_table)
        if downstream:
            for t in sorted(downstream):
                st.write(f"- {t}")
        else:
            st.write("_(none — nothing else depends on this)_")

    status = dq_status.get(selected_table)
    if status == "fail":
        st.error(f"`{selected_table}` currently has a FAILING check. "
                  f"See the Failure Drilldown page for the full blast radius.")
    elif status == "warn":
        st.warning(f"`{selected_table}` has a warning on its latest check.")
