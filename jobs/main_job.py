"""
Main PySpark job demonstrating usage of:
1. Third-party libraries (numpy, pandas, requests)
2. Custom shared libraries (shared_libs package)

All dependencies are available through the venv's Python interpreter -
no sys.path manipulation needed.
"""

import sys
import json
from datetime import datetime

import numpy as np
import pandas as pd
import requests

from shared_libs.constants.config import ENV, REGION, S3_BUCKET
from shared_libs.constants.casting_parameter import CastingParameter, SENSOR_TYPE_MAPPING
from shared_libs.models.sensor_model import SensorDataModel
from shared_libs.models.base_external_data_source import BaseExternalDataSource
from shared_libs.core_data_common_utils.data_quality import DataQualityChecker
from shared_libs.core_data_common_utils.spark_utils import get_spark_session
from shared_libs.core_data_source_utils.file_utils import get_hs_sensor_pl, get_ds_sensor_pl
from shared_libs.utils.logging_utils import get_logger
from shared_libs.utils.date_utils import get_current_timestamp

logger = get_logger("emr_poc_job")


def demonstrate_third_party_libs(spark):
    logger.info("=" * 60)
    logger.info("SECTION 1: Third-party Library Verification")
    logger.info("=" * 60)

    arr = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    logger.info(f"NumPy version: {np.__version__}")
    logger.info(f"NumPy array mean: {arr.mean()}, std: {arr.std():.4f}")

    pdf = pd.DataFrame({
        "sensor_id": [f"s_{i}" for i in range(5)],
        "value": np.random.randn(5),
        "timestamp": pd.date_range("2024-01-01", periods=5, freq="h"),
    })
    logger.info(f"Pandas version: {pd.__version__}")
    logger.info(f"Pandas DataFrame shape: {pdf.shape}")
    logger.info(f"Pandas DataFrame:\n{pdf.to_string()}")

    logger.info(f"Requests version: {requests.__version__}")

    spark_df = spark.createDataFrame(pdf[["sensor_id", "value"]])
    logger.info(f"Converted Pandas->Spark DataFrame count: {spark_df.count()}")
    spark_df.show()

    return True


def demonstrate_custom_libs(spark):
    logger.info("=" * 60)
    logger.info("SECTION 2: Custom Shared Library Verification")
    logger.info("=" * 60)

    logger.info(f"Environment: {ENV}, Region: {REGION}, Bucket: {S3_BUCKET}")
    logger.info(f"Sensor Type Mapping: {SENSOR_TYPE_MAPPING}")

    param = CastingParameter(
        source_column="raw_temp",
        target_column="temperature",
        target_type="double",
    )
    logger.info(f"CastingParameter: {param}")

    sensor_model = SensorDataModel(spark)
    logger.info(f"Sensor Model schema: {sensor_model.get_schema()}")
    logger.info(f"Sensor Model metadata: {sensor_model.get_metadata()}")

    sensor_df = sensor_model.read_data()
    logger.info("Sensor Data:")
    sensor_df.show()

    agg_df = sensor_model.compute_aggregations(sensor_df)
    logger.info("Aggregated Sensor Data by Location:")
    agg_df.show()

    dq_checker = DataQualityChecker()
    dq_result = dq_checker.run_all_checks(
        df=sensor_df,
        key_columns=["device_id"],
        check_columns=["temperature", "humidity", "pressure"],
    )
    logger.info(f"Data Quality Result: {json.dumps(dq_result, indent=2, default=str)}")

    trigger_time = "2024-01-01 10:00:00"
    hs_df = get_hs_sensor_pl(spark, trigger_time)
    logger.info("Historical Sensor Pipeline Data:")
    hs_df.show()

    ds_df = get_ds_sensor_pl(spark, trigger_time)
    logger.info("Downstream Sensor Pipeline Data:")
    ds_df.show()

    return True


def main():
    logger.info("=" * 60)
    logger.info("EMR PySpark Venv POC Job (Python 3.12)")
    logger.info(f"Start Time: {get_current_timestamp()}")
    logger.info(f"Python Version: {sys.version}")
    logger.info(f"Python Executable: {sys.executable}")
    logger.info("=" * 60)

    spark = get_spark_session("EMR_Venv_POC")

    try:
        tp_result = demonstrate_third_party_libs(spark)
        logger.info(f"Third-party libs test: {'PASSED' if tp_result else 'FAILED'}")

        cl_result = demonstrate_custom_libs(spark)
        logger.info(f"Custom libs test: {'PASSED' if cl_result else 'FAILED'}")

        logger.info("=" * 60)
        logger.info("FINAL RESULTS")
        logger.info("=" * 60)
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Third-party libraries (numpy, pandas, requests): {'PASSED' if tp_result else 'FAILED'}")
        logger.info(f"Custom shared libraries (shared_libs): {'PASSED' if cl_result else 'FAILED'}")
        logger.info(f"Overall: {'ALL PASSED' if (tp_result and cl_result) else 'SOME FAILED'}")
        logger.info(f"End Time: {get_current_timestamp()}")

    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
