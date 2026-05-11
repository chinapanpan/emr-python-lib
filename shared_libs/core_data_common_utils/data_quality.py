"""Data quality checking utilities."""

from typing import Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from shared_libs.constants.config import DATA_QUALITY_THRESHOLDS


class DataQualityChecker:
    """Performs data quality checks on DataFrames."""

    def __init__(self, thresholds: Optional[Dict] = None):
        self.thresholds = thresholds or DATA_QUALITY_THRESHOLDS
        self.results = []

    def check_null_ratio(self, df: DataFrame, columns: List[str]) -> Dict[str, float]:
        """Check null ratio for specified columns."""
        total_count = df.count()
        if total_count == 0:
            return {col: 0.0 for col in columns}

        null_ratios = {}
        for col in columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_ratios[col] = null_count / total_count

        return null_ratios

    def check_duplicates(self, df: DataFrame, key_columns: List[str]) -> float:
        """Check duplicate ratio based on key columns."""
        total = df.count()
        if total == 0:
            return 0.0
        distinct = df.select(key_columns).distinct().count()
        return (total - distinct) / total

    def run_all_checks(self, df: DataFrame, key_columns: List[str], check_columns: List[str]) -> Dict:
        """Run all data quality checks and return results."""
        null_ratios = self.check_null_ratio(df, check_columns)
        dup_ratio = self.check_duplicates(df, key_columns)
        row_count = df.count()

        passed = True
        issues = []

        for col, ratio in null_ratios.items():
            if ratio > self.thresholds["null_ratio_max"]:
                passed = False
                issues.append(f"Column '{col}' null ratio {ratio:.4f} exceeds threshold")

        if dup_ratio > self.thresholds["duplicate_ratio_max"]:
            passed = False
            issues.append(f"Duplicate ratio {dup_ratio:.4f} exceeds threshold")

        if row_count < self.thresholds["min_row_count"]:
            issues.append(f"Row count {row_count} below minimum threshold (warning)")

        result = {
            "passed": passed,
            "row_count": row_count,
            "null_ratios": null_ratios,
            "duplicate_ratio": dup_ratio,
            "issues": issues,
        }
        self.results.append(result)
        return result
