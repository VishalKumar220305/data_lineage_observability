"""
blast_radius.py

Given a table that has a failing DQ check, computes the full set of
downstream tables that are potentially affected -- this is the "blast
radius" the dashboard highlights, and what Day 6's failure-injection
scenarios will demonstrate concretely.
"""

import networkx as nx
from lineage.graph.lineage_graph import get_downstream


def compute_blast_radius(graph: nx.DiGraph, failing_table: str) -> dict:
    """
    Returns:
        {
            "failing_table": ...,
            "affected_tables": [...],   # sorted list of downstream tables
            "affected_count": int,
            "paths": {affected_table: [path from failing_table to it]},
        }
    """
    affected = get_downstream(graph, failing_table)

    paths = {}
    for table in affected:
        try:
            paths[table] = nx.shortest_path(graph, failing_table, table)
        except nx.NetworkXNoPath:
            paths[table] = []  # shouldn't happen since affected came from descendants()

    return {
        "failing_table": failing_table,
        "affected_tables": sorted(affected),
        "affected_count": len(affected),
        "paths": paths,
    }


def get_failing_tables_with_blast_radius(conn, graph: nx.DiGraph) -> list:
    """
    Looks up every table with a 'fail' status on its MOST RECENT check of
    each check_type, and computes blast radius for each. This is what the
    failure-drilldown dashboard page will call directly.

    Returns a list of dicts, one per (table, check_type) that is currently
    failing, each with the blast radius attached.
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT r1.table_name, r1.check_type, r1.column_name, r1.status, r1.detail, r1.checked_at
        FROM dq_check_results r1
        INNER JOIN (
            SELECT table_name, check_type, MAX(checked_at) AS max_checked_at
            FROM dq_check_results
            GROUP BY table_name, check_type
        ) latest
        ON r1.table_name = latest.table_name
           AND r1.check_type = latest.check_type
           AND r1.checked_at = latest.max_checked_at
        WHERE r1.status = 'fail'
    """)
    failing_checks = cursor.fetchall()
    cursor.close()

    results = []
    for check in failing_checks:
        blast = compute_blast_radius(graph, check["table_name"])
        results.append({**check, "blast_radius": blast})

    return results
