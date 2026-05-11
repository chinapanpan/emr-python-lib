"""Sensor data model implementation."""

from typing import Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

from shared_libs.models.base_external_data_source import BaseExternalDataSource
from shared_libs.constants.casting_parameter import SENSOR_TYPE_MAPPING


class SensorDataModel(BaseExternalDataSource):
    """Model for IoT sensor data processing."""

    def __init__(self, spark: SparkSession, source_name: str = "iot_sensors"):
        super().__init__(spark, source_name)

    def get_schema(self) -> Dict[str, str]:
        return SENSOR_TYPE_MAPPING

    def get_spark_struct_type(self) -> StructType:
        type_map = {
            "string": StringType(),
            "double": DoubleType(),
            "timestamp": TimestampType(),
            "integer": IntegerType(),
        }
        fields = [
            StructField(name, type_map.get(dtype, StringType()), True)
            for name, dtype in SENSOR_TYPE_MAPPING.items()
        ]
        return StructType(fields)

    def read_data(self, path: Optional[str] = None, **kwargs) -> DataFrame:
        """Read sensor data - generates sample data if no path provided."""
        if path:
            return self.spark.read.schema(self.get_spark_struct_type()).json(path)

        data = [
            ("sensor_001", "warehouse_A", 23.5, 65.2, 1013.25, "2024-01-01 10:00:00", 1),
            ("sensor_002", "warehouse_B", 21.8, 70.1, 1012.80, "2024-01-01 10:00:00", 1),
            ("sensor_003", "warehouse_A", 25.1, 55.8, 1014.10, "2024-01-01 10:01:00", 0),
            ("sensor_004", "warehouse_C", 19.3, 80.5, 1011.50, "2024-01-01 10:01:00", 1),
            ("sensor_005", "warehouse_B", 22.7, 68.9, 1013.00, "2024-01-01 10:02:00", 1),
        ]
        columns = ["device_id", "location", "temperature", "humidity", "pressure", "timestamp", "status"]
        return self.spark.createDataFrame(data, columns)

    def compute_aggregations(self, df: DataFrame) -> DataFrame:
        """Compute sensor aggregations by location."""
        return df.groupBy("location").agg(
            F.avg("temperature").alias("avg_temperature"),
            F.avg("humidity").alias("avg_humidity"),
            F.avg("pressure").alias("avg_pressure"),
            F.count("*").alias("reading_count"),
            F.sum(F.when(F.col("status") == 1, 1).otherwise(0)).alias("active_sensors"),
        )
