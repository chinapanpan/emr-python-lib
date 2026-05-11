"""Spark session and configuration utilities."""

from typing import Dict, Optional
from pyspark.sql import SparkSession

from shared_libs.constants.config import SPARK_CONFIG


def get_spark_session(app_name: str, config: Optional[Dict[str, str]] = None) -> SparkSession:
    """Create or get a SparkSession with standard configuration."""
    builder = SparkSession.builder.appName(app_name)

    merged_config = {**SPARK_CONFIG, **(config or {})}
    for key, value in merged_config.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def apply_spark_config(spark: SparkSession, config: Dict[str, str]) -> None:
    """Apply configuration to existing SparkSession."""
    for key, value in config.items():
        spark.conf.set(key, value)
