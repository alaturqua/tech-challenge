# FILE: test_post_prep_process_order.py

from datetime import datetime

import pytest
from modules.process_order import post_prep_process_order
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType


@pytest.fixture
def input_df(spark: SparkSession) -> DataFrame:
    """Create sample input DataFrame.

    Args:
        spark: SparkSession fixture

    Returns:
        DataFrame: Sample process order data
    """
    schema = StructType(
        [
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("AUFNR", StringType(), True),
            StructField("POSNR", StringType(), True),
            StructField("DWERK", StringType(), True),
            StructField("ZZGLTRP_ORIG", DateType(), True),
            StructField("LTRMI", DateType(), True),
            StructField("GSTRI", DateType(), True),
            StructField("KDAUF", StringType(), True),
        ]
    )

    data = [
        # On time MTO
        ("ERP1", "ORD1", "10", "PLANT1", datetime(2023, 1, 10), datetime(2023, 1, 15), datetime(2023, 1, 1), "SALES1"),
        # Late MTS
        ("ERP1", "ORD2", "20", "PLANT2", datetime(2023, 2, 25), datetime(2023, 2, 20), datetime(2023, 2, 1), None),
        # Very late MTO
        ("ERP1", "ORD3", "30", "PLANT1", datetime(2023, 3, 25), datetime(2023, 3, 5), datetime(2023, 3, 1), "SALES2"),
        # Null dates
        ("ERP1", "ORD4", "40", "PLANT2", None, None, None, None),
    ]

    return spark.createDataFrame(data, schema)


def test_post_prep_process_order_primary_keys(input_df: DataFrame):
    """Test primary key generation.

    Args:
        input_df: Input DataFrame fixture
    """
    result = post_prep_process_order(input_df)

    row = result.filter(result.AUFNR == "ORD1").first()
    assert row["primary_key_intra"] == "ORD1_10_PLANT1"
    assert row["primary_key_inter"] == "ERP1_ORD1_10_PLANT1"


def test_post_prep_process_order_on_time_calcs(input_df: DataFrame):
    """Test on-time delivery calculations.

    Args:
        input_df: Input DataFrame fixture
    """
    result = post_prep_process_order(input_df)

    # On time case
    on_time = result.filter(result.AUFNR == "ORD1").first()
    assert on_time["on_time_flag"] == 0
    assert on_time["actual_on_time_deviation"] == -5

    # Late case
    late = result.filter(result.AUFNR == "ORD2").first()
    assert late["on_time_flag"] == 1
    assert late["actual_on_time_deviation"] == 5


def test_post_prep_process_order_late_buckets(input_df: DataFrame):
    """Test late delivery bucket categorization.

    Args:
        input_df: Input DataFrame fixture
    """
    result = post_prep_process_order(input_df)

    assert result.filter(result.AUFNR == "ORD1").first()["late_delivery_bucket"] == "On Time"
    assert result.filter(result.AUFNR == "ORD2").first()["late_delivery_bucket"] == "1-7 Days Late"
    assert result.filter(result.AUFNR == "ORD3").first()["late_delivery_bucket"] == ">14 Days Late"
    assert result.filter(result.AUFNR == "ORD4").first()["late_delivery_bucket"] == "Unknown"


def test_post_prep_process_order_mto_mts(input_df: DataFrame):
    """Test MTO vs MTS flag derivation.

    Args:
        input_df: Input DataFrame fixture
    """
    result = post_prep_process_order(input_df)

    assert result.filter(result.KDAUF == "SALES1").first()["mto_vs_mts_flag"] == "MTO"
    assert result.filter(result.KDAUF.isNull()).first()["mto_vs_mts_flag"] == "MTS"


def test_post_prep_process_order_null_handling(input_df: DataFrame):
    """Test handling of null values.

    Args:
        input_df: Input DataFrame fixture
    """
    result = post_prep_process_order(input_df)
    null_row = result.filter(result.AUFNR == "ORD4").first()

    assert null_row["on_time_flag"] is None
    assert null_row["actual_on_time_deviation"] is None
    assert null_row["order_finish_timestamp"] is None
    assert null_row["order_start_timestamp"] is None
