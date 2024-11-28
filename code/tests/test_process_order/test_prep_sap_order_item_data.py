"""Tests for prep_sap_order_item function in process_order module."""

import pytest
from modules.process_order import prep_sap_order_item
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType


def test_prep_sap_order_item_valid_input(spark: SparkSession):
    """Test prep_sap_order_item with valid input data."""
    # Setup schema
    schema = StructType(
        [
            StructField("AUFNR", StringType(), True),
            StructField("POSNR", LongType(), True),
            StructField("DWERK", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("MEINS", StringType(), True),
            StructField("KDAUF", StringType(), True),
            StructField("KDPOS", StringType(), True),
            StructField("LTRMI", StringType(), True),
            StructField("EXTRA_COL", StringType(), True),  # Extra column that should be filtered out
        ]
    )

    # Test data
    data = [
        ("ORDER1", 10, "PLANT1", "MAT1", "PC", "SALES1", "ITEM1", "2023-10-01", "EXTRA1"),
        ("ORDER2", 20, "PLANT2", "MAT2", "KG", "SALES2", "ITEM2", "2023-10-02", "EXTRA2"),
    ]

    input_df = spark.createDataFrame(data, schema)
    result = prep_sap_order_item(input_df)

    # Verify results
    assert result.count() == 2
    assert len(result.columns) == 8  # Check only required columns are present
    required_columns = ["AUFNR", "POSNR", "DWERK", "MATNR", "MEINS", "KDAUF", "KDPOS", "LTRMI"]
    assert all(col in result.columns for col in required_columns)


def test_prep_sap_order_item_empty_df(spark: SparkSession):
    """Test prep_sap_order_item with empty DataFrame."""
    # Setup schema
    schema = StructType(
        [
            StructField("AUFNR", StringType(), True),
            StructField("POSNR", LongType(), True),
            StructField("DWERK", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("MEINS", StringType(), True),
            StructField("KDAUF", StringType(), True),
            StructField("KDPOS", StringType(), True),
            StructField("LTRMI", StringType(), True),
        ]
    )

    input_df = spark.createDataFrame([], schema)
    result = prep_sap_order_item(input_df)

    assert result.count() == 0
    assert len(result.columns) == 8


def test_prep_sap_order_item_invalid_input():
    # Test None input
    with pytest.raises(TypeError, match="Invalid input DataFrame"):
        prep_sap_order_item(None)

    # Test invalid input type
    with pytest.raises(TypeError, match="Invalid input DataFrame"):
        prep_sap_order_item("not a dataframe")


def test_prep_sap_order_item_null_values(spark):
    """Test prep_sap_order_item with null values in input data."""
    # Setup schema with nullable fields
    schema = StructType(
        [
            StructField("AUFNR", StringType(), True),
            StructField("POSNR", LongType(), True),
            StructField("DWERK", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("MEINS", StringType(), True),
            StructField("KDAUF", StringType(), True),
            StructField("KDPOS", StringType(), True),
            StructField("LTRMI", StringType(), True),
        ]
    )

    # Test data with null values
    data = [("ORDER1", 10, None, "MAT1", None, "SALES1", None, None)]

    input_df = spark.createDataFrame(data, schema)
    result = prep_sap_order_item(input_df)

    assert result.count() == 1
    assert result.select("DWERK").first()[0] is None
