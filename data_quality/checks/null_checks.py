"""
null_checks.py

For each configured column, computes what fraction of rows are NULL and
compares it against the configured threshold (data_quality.null_checks
.max_null_rate_pct in config.yaml).
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def check_nulls(df: DataFrame, columns: list, max_null_rate_pct: float) -> list:
    """
    Returns a list of result dicts, one per column:
        {"check_type": "null_check", "column_name": ..., "status": "pass"|"fail",
         "detail": "..."}
    """
    results = []
    total_rows = df.count()

    for column in columns:
        if column not in df.columns:
            results.append({
                "check_type": "null_check",
                "column_name": column,
                "status": "fail",
                "detail": f"column '{column}' not found in table (schema drift?)",
            })
            continue

        null_count = df.filter(col(column).isNull()).count()
        null_rate_pct = (null_count / total_rows * 100) if total_rows > 0 else 0.0

        status = "fail" if null_rate_pct > max_null_rate_pct else "pass"
        detail = (f"null_rate={null_rate_pct:.2f}%, threshold={max_null_rate_pct}%, "
                  f"null_count={null_count}, total_rows={total_rows}")

        results.append({
            "check_type": "null_check",
            "column_name": column,
            "status": status,
            "detail": detail,
        })

    return results
