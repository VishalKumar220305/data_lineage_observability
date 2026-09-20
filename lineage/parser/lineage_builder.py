"""
lineage_builder.py

Runs the AST extractor (ast_extractor.py) across every job listed in
config.yaml's pipeline_jobs section, and writes the resolved source->target
edges into the lineage_edges table.

Any unresolved cases are NOT silently dropped -- they're printed clearly so
you know exactly which jobs need a manual entry in lineage_overrides, and
why the parser couldn't resolve them automatically.

Usage:
    python -m lineage.parser.lineage_builder
"""

import os
import yaml
import mysql.connector
from dotenv import load_dotenv

from lineage.parser.ast_extractor import extract_lineage_from_file

load_dotenv()


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


def _write_edges(conn, job_name: str, edges: list):
    """Writes edges for one job. Uses ON DUPLICATE KEY UPDATE so re-running
    the parser after code changes doesn't create duplicate rows -- it just
    refreshes detected_at for edges that still exist."""
    cursor = conn.cursor()
    for source, target in edges:
        cursor.execute(
            """
            INSERT INTO lineage_edges (job_name, source_table, target_table, source_type)
            VALUES (%s, %s, %s, 'auto_parsed')
            ON DUPLICATE KEY UPDATE detected_at = CURRENT_TIMESTAMP
            """,
            (job_name, source, target),
        )
    conn.commit()
    cursor.close()


def run():
    config = _load_config()
    conn = _get_connection()

    total_edges = 0
    all_unresolved = []

    print("=" * 70)
    print("Running AST-based lineage extraction across all configured jobs")
    print("=" * 70)

    for job in config["pipeline_jobs"]:
        job_name = job["name"]
        script_path = job["script_path"]

        if not os.path.exists(script_path):
            print(f"[SKIP] {job_name}: script not found at {script_path}")
            continue

        result = extract_lineage_from_file(script_path)

        if result.edges:
            _write_edges(conn, job_name, result.edges)
            total_edges += len(result.edges)
            print(f"\n[{job_name}] {len(result.edges)} edge(s) resolved and written:")
            for source, target in result.edges:
                print(f"    {source}  -->  {target}")

        if result.unresolved:
            print(f"\n[{job_name}] {len(result.unresolved)} UNRESOLVED case(s):")
            for item in result.unresolved:
                print(f"    target='{item['target']}'  reason: {item['reason']}")
            all_unresolved.extend(
                {"job_name": job_name, **item} for item in result.unresolved
            )

    conn.close()

    print("\n" + "=" * 70)
    print(f"Done. {total_edges} edge(s) written to lineage_edges.")
    if all_unresolved:
        print(f"\n{len(all_unresolved)} case(s) need a manual entry in "
              f"lineage_overrides -- see details above.")
        print("Nothing was guessed or silently skipped; these are flagged, not lost.")
    else:
        print("No unresolved cases -- every read/write pattern was traced automatically.")
    print("=" * 70)


if __name__ == "__main__":
    run()
