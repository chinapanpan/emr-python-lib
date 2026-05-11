"""Configuration constants for data processing pipelines."""

ENV = "prod"
REGION = "ap-southeast-1"

DATABASE_CONFIG = {
    "host": "data-warehouse.example.com",
    "port": 5432,
    "database": "analytics",
}

S3_BUCKET = "zpfsingapore"
S3_PREFIX = "emr/data"

SPARK_CONFIG = {
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}

DATA_QUALITY_THRESHOLDS = {
    "null_ratio_max": 0.05,
    "duplicate_ratio_max": 0.01,
    "min_row_count": 100,
}
