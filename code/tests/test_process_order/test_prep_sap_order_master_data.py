"""Tests for prep_sap_order_master_data function."""

from datetime import datetime

import pytest
from modules.process_order import prep_sap_order_master_data
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType


def test_prep_sap_order_master_data_valid_input(spark: SparkSession):
    """Test with valid input data."""
    schema = StructType(
        [
            StructField("AUFNR", StringType(), True),
            StructField("OBJNR", StringType(), True),
            StructField("ERDAT", DateType(), True),
            StructField("ERNAM", StringType(), True),
            StructField("AUART", StringType(), True),
            StructField("ZZGLTRP_ORIG", StringType(), True),
            StructField("ZZPRO_TEXT", StringType(), True),
            StructField("EXTRA_COL", StringType(), True),
        ]
    )

    data = [("ORDER1", "OBJ1", datetime(2023, 1, 1), "USER1", "TYPE1", "ORIG1", "PROJECT1", "EXTRA1")]

    df = spark.createDataFrame(data, schema)
    result = prep_sap_order_master_data(df)

    assert result.count() == 1
    assert len(result.columns) == 7
    expected_columns = ["AUFNR", "OBJNR", "ERDAT", "ERNAM", "AUART", "ZZGLTRP_ORIG", "ZZPRO_TEXT"]
    assert all(col in result.columns for col in expected_columns)


def test_prep_sap_order_master_data_empty_df(spark: SparkSession):
    """Test with empty DataFrame."""
    schema = StructType(
        [
            StructField("AUFNR", StringType(), True),
            StructField("OBJNR", StringType(), True),
            StructField("ERDAT", DateType(), True),
            StructField("ERNAM", StringType(), True),
            StructField("AUART", StringType(), True),
            StructField("ZZGLTRP_ORIG", StringType(), True),
            StructField("ZZPRO_TEXT", StringType(), True),
        ]
    )

    df = spark.createDataFrame([], schema)
    result = prep_sap_order_master_data(df)

    assert result.count() == 0
    assert len(result.columns) == 7


def test_prep_sap_order_master_data_invalid_input():
    """Test with invalid input."""
    with pytest.raises(TypeError, match="Invalid input DataFrame"):
        prep_sap_order_master_data(None)

    with pytest.raises(TypeError, match="Invalid input DataFrame"):
        prep_sap_order_master_data("not a dataframe")


def test_prep_sap_order_master_data_null_values(spark: SparkSession):
    """Test with null values."""
    schema = StructType(
        [
            StructField("AUFNR", StringType(), True),
            StructField("OBJNR", StringType(), True),
            StructField("ERDAT", DateType(), True),
            StructField("ERNAM", StringType(), True),
            StructField("AUART", StringType(), True),
            StructField("ZZGLTRP_ORIG", StringType(), True),
            StructField("ZZPRO_TEXT", StringType(), True),
        ]
    )

    data = [(None, None, None, None, None, None, None)]
    df = spark.createDataFrame(data, schema)
    result = prep_sap_order_master_data(df)

    assert result.count() == 1
    for col in result.columns:
        assert result.select(col).first()[0] is None
