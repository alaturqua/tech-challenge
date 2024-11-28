"""Tests for prep_valuation_data function in local_material module."""

import pytest
from modules.local_material import prep_valuation_data
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual


def test_prep_valuation_data_valid_input(spark: SparkSession):
    """Test prep_valuation_data with valid input data."""
    data = [
        ("100", "VAL1", "COMP1"),
        ("100", "VAL2", "COMP2"),
    ]
    schema = "MANDT STRING, BWKEY STRING, BUKRS STRING"
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [
        ("100", "VAL1", "COMP1"),
        ("100", "VAL2", "COMP2"),
    ]
    expected_schema = "MANDT STRING, BWKEY STRING, BUKRS STRING"
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_valuation_data(input_df)

    assertDataFrameEqual(result_df, expected_df), "DataFrames are not equal"


def test_prep_valuation_data_invalid_input():
    """Test prep_valuation_data with invalid input type."""
    with pytest.raises(TypeError, match="df must be a DataFrame"):
        prep_valuation_data(None)


def test_prep_valuation_data_missing_columns(spark: SparkSession):
    """Test prep_valuation_data with missing columns in input DataFrame."""
    data = [
        ("100", "VAL1"),
        ("100", "VAL2"),
    ]
    schema = "MANDT STRING, BWKEY STRING"
    input_df = spark.createDataFrame(data, schema=schema)

    with pytest.raises(Exception):
        prep_valuation_data(input_df)


def test_prep_valuation_data_duplicates(spark: SparkSession):
    """Test prep_valuation_data with duplicate records."""
    data = [
        ("100", "VAL1", "COMP1"),
        ("100", "VAL1", "COMP1"),
    ]
    schema = "MANDT STRING, BWKEY STRING, BUKRS STRING"
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [
        ("100", "VAL1", "COMP1"),
    ]
    expected_schema = "MANDT STRING, BWKEY STRING, BUKRS STRING"
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_valuation_data(input_df)

    assertDataFrameEqual(result_df, expected_df), "DataFrames are not equal"
