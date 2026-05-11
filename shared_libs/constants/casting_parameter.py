"""Casting parameters and type mapping for data source processing."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


SENSOR_TYPE_MAPPING = {
    "temperature": "double",
    "humidity": "double",
    "pressure": "double",
    "timestamp": "timestamp",
    "device_id": "string",
    "location": "string",
    "status": "integer",
}

DATE_FORMAT = "yyyy-MM-dd"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


@dataclass
class CastingParameter:
    source_column: str
    target_column: str
    target_type: str
    nullable: bool = True
    default_value: Optional[str] = None


@dataclass
class TableSchema:
    table_name: str
    database: str
    columns: List[CastingParameter] = field(default_factory=list)
    partition_keys: List[str] = field(default_factory=list)

    def get_spark_schema(self) -> Dict[str, str]:
        return {col.target_column: col.target_type for col in self.columns}
