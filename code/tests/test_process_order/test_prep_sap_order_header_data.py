"""Tests for prep_sap_order_header_data function."""

from datetime import datetime

import pytest
from modules.process_order import prep_sap_order_header_data
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType


def test_prep_sap_order_header_data_valid_input(spark: SparkSession):
    """Test prep_sap_order_header_data with valid input data."""
    schema = StructType(
        [
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("MANDT", StringType(), True),
            StructField("AUFNR", StringType(), True),
            StructField("GLTRP", DateType(), True),
            StructField("GSTRP", DateType(), True),
            StructField("FTRMS", DateType(), True),
            StructField("GLTRS", DateType(), True),
            StructField("GSTRS", DateType(), True),
            StructField("GSTRI", DateType(), True),
            StructField("GETRI", DateType(), True),
            StructField("GLTRI", DateType(), True),
            StructField("FTRMI", DateType(), True),
            StructField("FTRMP", DateType(), True),
            StructField("DISPO", StringType(), True),
            StructField("FEVOR", StringType(), True),
            StructField("PLGRP", StringType(), True),
            StructField("FHORI", StringType(), True),
            StructField("AUFPL", StringType(), True),
        ]
    )

    data = [
        (
            "ERP1",
            "100",
            "ORDER1",
            datetime(2023, 12, 31),
            datetime(2023, 1, 1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "D1",
            "S1",
            "P1",
            "H1",
            "A1",
        )
    ]

    df = spark.createDataFrame(data, schema)
    result = prep_sap_order_header_data(df)

    assert result.count() == 1
    assert "start_date" in result.columns
    assert "finish_date" in result.columns


def test_prep_sap_order_header_data_null_dates(spark: SparkSession):
    """Test prep_sap_order_header_data with null dates."""
    schema = StructType(
        [
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("MANDT", StringType(), True),
            StructField("AUFNR", StringType(), True),
            StructField("GLTRP", DateType(), True),
            StructField("GSTRP", DateType(), True),
            StructField("FTRMS", DateType(), True),
            StructField("GLTRS", DateType(), True),
            StructField("GSTRS", DateType(), True),
            StructField("GSTRI", DateType(), True),
            StructField("GETRI", DateType(), True),
            StructField("GLTRI", DateType(), True),
            StructField("FTRMI", DateType(), True),
            StructField("FTRMP", DateType(), True),
            StructField("DISPO", StringType(), True),
            StructField("FEVOR", StringType(), True),
            StructField("PLGRP", StringType(), True),
            StructField("FHORI", StringType(), True),
            StructField("AUFPL", StringType(), True),
        ]
    )

    data = [
        (
            "ERP1",
            "100",
            "ORDER1",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "D1",
            "S1",
            "P1",
            "H1",
            "A1",
        )
    ]

    df = spark.createDataFrame(data, schema)
    result = prep_sap_order_header_data(df)

    assert result.count() == 1
    assert not result.select("start_date").first()[0] is None
    assert not result.select("finish_date").first()[0] is None


def test_prep_sap_order_header_data_empty_df(spark: SparkSession):
    """Test prep_sap_order_header_data with empty DataFrame."""
    schema = StructType(
        [
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("MANDT", StringType(), True),
            StructField("AUFNR", StringType(), True),
            StructField("GLTRP", DateType(), True),
            StructField("GSTRP", DateType(), True),
            StructField("FTRMS", DateType(), True),
            StructField("GLTRS", DateType(), True),
            StructField("GSTRS", DateType(), True),
            StructField("GSTRI", DateType(), True),
            StructField("GETRI", DateType(), True),
            StructField("GLTRI", DateType(), True),
            StructField("FTRMI", DateType(), True),
            StructField("FTRMP", DateType(), True),
            StructField("DISPO", StringType(), True),
            StructField("FEVOR", StringType(), True),
            StructField("PLGRP", StringType(), True),
            StructField("FHORI", StringType(), True),
            StructField("AUFPL", StringType(), True),
        ]
    )

    df = spark.createDataFrame([], schema)
    result = prep_sap_order_header_data(df)

    assert result.count() == 0


def test_prep_sap_order_header_data_invalid_input():
    """Test prep_sap_order_header_data with invalid input data."""
    with pytest.raises(TypeError):
        prep_sap_order_header_data(None)

    with pytest.raises(TypeError):
        prep_sap_order_header_data("not a dataframe")
