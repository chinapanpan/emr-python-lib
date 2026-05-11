"""S3 data loading utilities."""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession


class S3DataLoader:
    """Utility class for loading data from S3."""

    def __init__(self, spark: SparkSession, bucket: str, prefix: str = ""):
        self.spark = spark
        self.bucket = bucket
        self.prefix = prefix

    def _build_path(self, key: str) -> str:
        if self.prefix:
            return f"s3://{self.bucket}/{self.prefix}/{key}"
        return f"s3://{self.bucket}/{key}"

    def read_parquet(self, key: str) -> DataFrame:
        """Read parquet data from S3."""
        path = self._build_path(key)
        return self.spark.read.parquet(path)

    def read_json(self, key: str) -> DataFrame:
        """Read JSON data from S3."""
        path = self._build_path(key)
        return self.spark.read.json(path)

    def read_csv(self, key: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
        """Read CSV data from S3."""
        path = self._build_path(key)
        return self.spark.read.csv(path, header=header, inferSchema=infer_schema)

    def write_parquet(self, df: DataFrame, key: str, mode: str = "overwrite",
                      partition_by: Optional[list] = None) -> None:
        """Write DataFrame to S3 as parquet."""
        path = self._build_path(key)
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(path)
