"""Base class for external data source definitions."""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession


class BaseExternalDataSource(ABC):
    """Base class for all external data source connectors."""

    def __init__(self, spark: SparkSession, source_name: str):
        self.spark = spark
        self.source_name = source_name
        self._schema = None

    @abstractmethod
    def get_schema(self) -> Dict[str, str]:
        """Return the schema definition for this data source."""
        pass

    @abstractmethod
    def read_data(self, **kwargs) -> DataFrame:
        """Read data from the external source."""
        pass

    def validate_data(self, df: DataFrame) -> bool:
        """Validate the dataframe meets quality thresholds."""
        if df.count() == 0:
            return False
        return True

    def apply_casting(self, df: DataFrame, casting_params: List) -> DataFrame:
        """Apply type casting based on casting parameters."""
        for param in casting_params:
            df = df.withColumn(
                param.target_column,
                df[param.source_column].cast(param.target_type)
            )
        return df

    def get_metadata(self) -> Dict[str, str]:
        """Return metadata about this data source."""
        return {
            "source_name": self.source_name,
            "schema": str(self.get_schema()),
        }
