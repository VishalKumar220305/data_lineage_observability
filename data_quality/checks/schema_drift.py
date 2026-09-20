"""
schema_drift.py

Compares a table's CURRENT schema (column name -> data type) against the
LAST schema snapshot recorded for that table (fetched by dq_runner.py from
the most recent schema_drift row in dq_check_results). This module does no
database I/O itself -- it's pure comparison logic, so it's easy to test in
isolation.

If there's no previous snapshot (first time this table has been checked),
the current schema becomes the baseline and the check passes by definition
-- there's nothing to have drifted from yet.
"""

import json


def check_schema_drift(current_schema: dict, previous_schema_json: str,
                        fail_on_column_removed: bool, fail_on_type_changed: bool,
                        warn_on_column_added: bool) -> dict:
    """
    current_schema: dict of {column_name: type_string} for the table right now.
    previous_schema_json: JSON string of the last recorded schema, or None if
        this is the first time this table has been checked.

    Returns a single result dict:
        {"check_type": "schema_drift", "column_name": None, "status": "pass"|"fail"|"warn",
         "detail": "..."}
    The detail field always contains the CURRENT schema as JSON -- this is
    what becomes the new baseline for the next comparison.
    """
    current_schema_json = json.dumps(current_schema, sort_keys=True)

    if previous_schema_json is None:
        return {
            "check_type": "schema_drift",
            "column_name": None,
            "status": "pass",
            "detail": f"no prior snapshot -- recording baseline: {current_schema_json}",
        }

    previous_schema = json.loads(previous_schema_json)

    removed_columns = set(previous_schema.keys()) - set(current_schema.keys())
    added_columns = set(current_schema.keys()) - set(previous_schema.keys())
    type_changed_columns = {
        col: (previous_schema[col], current_schema[col])
        for col in (set(previous_schema.keys()) & set(current_schema.keys()))
        if previous_schema[col] != current_schema[col]
    }

    issues = []
    status = "pass"

    if removed_columns:
        issues.append(f"removed columns: {sorted(removed_columns)}")
        if fail_on_column_removed:
            status = "fail"

    if type_changed_columns:
        issues.append(f"type changes: {type_changed_columns}")
        if fail_on_type_changed:
            status = "fail"

    if added_columns:
        issues.append(f"added columns: {sorted(added_columns)}")
        if warn_on_column_added and status == "pass":
            status = "warn"

    detail = "; ".join(issues) if issues else "no schema changes detected"
    detail += f" | current_schema: {current_schema_json}"

    return {
        "check_type": "schema_drift",
        "column_name": None,
        "status": status,
        "detail": detail,
    }
