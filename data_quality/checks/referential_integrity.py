"""
referential_integrity.py

Checks that every non-null value in a child table's FK column actually
exists in the referenced parent table's column -- i.e. no orphan rows.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def check_referential_integrity(child_df: DataFrame, fk_column: str,
                                 parent_df: DataFrame, parent_column: str,
                                 fail_on_any_orphan: bool) -> dict:
    """
    Returns a single result dict:
        {"check_type": "referential_integrity", "column_name": fk_column,
         "status": "pass"|"fail", "detail": "..."}
    """
    child_keys = child_df.filter(col(fk_column).isNotNull()).select(fk_column).distinct()
    parent_keys = parent_df.select(parent_column).distinct()

    orphan_keys = child_keys.join(
        parent_keys,
        child_keys[fk_column] == parent_keys[parent_column],
        how="left_anti",
    )
    orphan_count = orphan_keys.count()

    if fail_on_any_orphan:
        status = "fail" if orphan_count > 0 else "pass"
    else:
        status = "warn" if orphan_count > 0 else "pass"

    detail = (f"orphan_key_count={orphan_count} (distinct '{fk_column}' values with no "
              f"matching '{parent_column}' in parent table)")

    return {
        "check_type": "referential_integrity",
        "column_name": fk_column,
        "status": status,
        "detail": detail,
    }
