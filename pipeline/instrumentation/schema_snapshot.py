"""
schema_snapshot.py

Captures the column names + data types of a PySpark DataFrame right before
it's written out. This is the raw material the Day 3 schema-drift check will
compare between runs to detect "a column disappeared" or "a type changed."

For Day 1, this just captures and prints the snapshot as a sanity check —
Day 3 will add the comparison logic and wire it into dq_check_results.
"""

import json
from pyspark.sql import DataFrame


def capture_schema(df: DataFrame, table_name: str) -> dict:
    """
    Returns a simple {column_name: data_type} snapshot of a DataFrame's schema.
    Kept intentionally simple (just name + type) since that's all the
    schema-drift check in Day 3 needs to compare.
    """
    snapshot = {field.name: str(field.dataType) for field in df.schema.fields}

    print(f"[SchemaSnapshot] {table_name}: {json.dumps(snapshot, indent=2)}")
    return snapshot


def snapshot_to_json(snapshot: dict) -> str:
    """Serializes a snapshot dict to a JSON string for storage (Day 3 will
    store these so drift checks have something to compare against)."""
    return json.dumps(snapshot, sort_keys=True)
