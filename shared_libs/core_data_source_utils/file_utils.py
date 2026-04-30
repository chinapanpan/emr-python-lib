"""File utility functions for data source pipeline."""

from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shared_libs.constants.casting_parameter import CastingParameter, TableSchema


def get_hs_sensor_pl(spark: SparkSession, trigger_time: str, timeline_duration: int = 60) -> DataFrame:
    """Get historical sensor pipeline data.

    Simulates fetching historical sensor readings for a given time window.
    """
    data = [
        (trigger_time, "sensor_hs_001", "zone_A", 22.5, 60.0, timeline_duration),
        (trigger_time, "sensor_hs_002", "zone_B", 24.1, 55.3, timeline_duration),
        (trigger_time, "sensor_hs_003", "zone_A", 21.8, 62.7, timeline_duration),
        (trigger_time, "sensor_hs_004", "zone_C", 26.3, 48.9, timeline_duration),
    ]
    columns = ["trigger_time", "device_id", "zone", "temperature", "humidity", "duration_minutes"]
    df = spark.createDataFrame(data, columns)
    return df.withColumn("trigger_time", F.to_timestamp(F.col("trigger_time")))


def get_ds_sensor_pl(spark: SparkSession, trigger_time: str, timeline_duration: int = 60) -> DataFrame:
    """Get downstream sensor pipeline data.

    Simulates fetching downstream processed sensor data.
    """
    data = [
        (trigger_time, "ds_001", "processed", 23.2, "NORMAL", timeline_duration),
        (trigger_time, "ds_002", "processed", 25.8, "WARNING", timeline_duration),
        (trigger_time, "ds_003", "raw", 20.1, "NORMAL", timeline_duration),
    ]
    columns = ["trigger_time", "pipeline_id", "stage", "avg_temp", "alert_status", "window_minutes"]
    df = spark.createDataFrame(data, columns)
    return df.withColumn("trigger_time", F.to_timestamp(F.col("trigger_time")))


def build_table_schema(table_name: str, database: str, columns_config: List[Dict]) -> TableSchema:
    """Build a TableSchema from configuration."""
    casting_params = [
        CastingParameter(
            source_column=col["source"],
            target_column=col["target"],
            target_type=col["type"],
            nullable=col.get("nullable", True),
        )
        for col in columns_config
    ]
    return TableSchema(table_name=table_name, database=database, columns=casting_params)
