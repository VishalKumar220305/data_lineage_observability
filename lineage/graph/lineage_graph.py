"""
lineage_graph.py

Builds a networkx DiGraph from the metadata store: nodes are tables, edges
are source -> target lineage relationships pulled from BOTH lineage_edges
(auto-parsed) and lineage_overrides (manually added) -- combining both is
what makes the graph complete even where the AST parser had gaps.
"""

import networkx as nx


def build_graph_from_edges(auto_edges: list, override_edges: list) -> nx.DiGraph:
    """
    auto_edges / override_edges: lists of (source, target, job_name) tuples.

    Returns a networkx DiGraph. Each edge carries:
      - job_name: which job produced this edge
      - source_type: 'auto_parsed' or 'manual_override'
    Each node carries no attributes by default -- callers (e.g. the
    dashboard) attach DQ status separately, since that's a display concern,
    not a structural one.
    """
    graph = nx.DiGraph()

    for source, target, job_name in auto_edges:
        graph.add_edge(source, target, job_name=job_name, source_type="auto_parsed")

    for source, target, job_name in override_edges:
        # A manual override for a pair that's already auto-parsed just
        # confirms/refreshes it -- don't create a duplicate edge, but do
        # let the override's source_type win, since a human confirmed it.
        graph.add_edge(source, target, job_name=job_name, source_type="manual_override")

    return graph


def build_graph_from_db(conn) -> nx.DiGraph:
    """Fetches edges from MySQL and builds the graph. This is the version
    the dashboard actually calls."""
    cursor = conn.cursor()

    cursor.execute("SELECT source_table, target_table, job_name FROM lineage_edges")
    auto_edges = cursor.fetchall()

    cursor.execute("SELECT source_table, target_table, job_name FROM lineage_overrides")
    override_edges = cursor.fetchall()

    cursor.close()
    return build_graph_from_edges(auto_edges, override_edges)


def get_upstream(graph: nx.DiGraph, table_name: str) -> set:
    """All tables that eventually feed INTO table_name (ancestors)."""
    if table_name not in graph:
        return set()
    return nx.ancestors(graph, table_name)


def get_downstream(graph: nx.DiGraph, table_name: str) -> set:
    """All tables that eventually receive data FROM table_name (descendants).
    This is the core of blast-radius: everything downstream of a failure."""
    if table_name not in graph:
        return set()
    return nx.descendants(graph, table_name)
