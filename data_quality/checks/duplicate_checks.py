"""
duplicate_checks.py

Checks what fraction of rows share the same key column(s) with at least one
other row -- i.e. true duplicate keys, not just duplicate content.
"""

from pyspark.sql import DataFrame


def check_duplicates(df: DataFrame, key_columns: list, max_duplicate_rate_pct: float) -> dict:
    """
    Returns a single result dict:
        {"check_type": "duplicate_check", "column_name": "<key columns joined>",
         "status": "pass"|"fail", "detail": "..."}
    """
    total_rows = df.count()
    distinct_key_rows = df.select(*key_columns).distinct().count()
    duplicate_count = total_rows - distinct_key_rows
    duplicate_rate_pct = (duplicate_count / total_rows * 100) if total_rows > 0 else 0.0

    status = "fail" if duplicate_rate_pct > max_duplicate_rate_pct else "pass"
    detail = (f"duplicate_rate={duplicate_rate_pct:.2f}%, threshold={max_duplicate_rate_pct}%, "
              f"duplicate_count={duplicate_count}, total_rows={total_rows}")

    return {
        "check_type": "duplicate_check",
        "column_name": ",".join(key_columns),
        "status": status,
        "detail": detail,
    }
