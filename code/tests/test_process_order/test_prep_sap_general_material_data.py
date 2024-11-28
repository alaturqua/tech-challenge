"""Tests for prep_sap_general_material_data function."""

import pytest
from modules.process_order import prep_sap_general_material_data
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


def test_prep_sap_general_material_data_valid_input(spark: SparkSession):
    """Test prep_sap_general_material_data with valid input data."""
    schema = StructType(
        [
            StructField("MATNR", StringType(), True),
            StructField("GLOBAL_MAT", StringType(), True),
            StructField("NTGEW", DoubleType(), True),
            StructField("MTART", StringType(), True),
            StructField("BISMT", StringType(), True),
            StructField("LVORM", StringType(), True),
        ]
    )

    data = [("MAT1", "GMAT1", 10.5, "TYPE1", None, None), ("MAT2", "GMAT2", 20.0, "TYPE2", "VALID", "")]

    df = spark.createDataFrame(data, schema)
    result = prep_sap_general_material_data(df, "GLOBAL_MAT")

    assert result.count() == 2
    assert set(result.columns) == {"MATNR", "GLOBAL_MATERIAL_NUMBER", "NTGEW", "MTART"}
    first_row = result.first()
    assert first_row["MATNR"] == "MAT1"
    assert first_row["GLOBAL_MATERIAL_NUMBER"] == "GMAT1"


def test_prep_sap_general_material_data_filtering(spark: SparkSession):
    """Test prep_sap_general_material_data with filtering."""
    schema = StructType(
        [
            StructField("MATNR", StringType(), True),
            StructField("GLOBAL_MAT", StringType(), True),
            StructField("NTGEW", DoubleType(), True),
            StructField("MTART", StringType(), True),
            StructField("BISMT", StringType(), True),
            StructField("LVORM", StringType(), True),
        ]
    )

    data = [
        ("MAT1", "GMAT1", 10.5, "TYPE1", None, None),  # Should be included
        ("MAT2", "GMAT2", 20.0, "TYPE2", "ARCHIVE", None),  # Should be excluded
        ("MAT3", "GMAT3", 30.0, "TYPE3", "DUPLICATE", ""),  # Should be excluded
        ("MAT4", "GMAT4", 40.0, "TYPE4", None, "X"),  # Should be excluded
    ]

    df = spark.createDataFrame(data, schema)
    result = prep_sap_general_material_data(df, "GLOBAL_MAT")

    assert result.count() == 1
    assert result.first()["MATNR"] == "MAT1"


def test_prep_sap_general_material_data_empty_df(spark: SparkSession):
    schema = StructType(
        [
            StructField("MATNR", StringType(), True),
            StructField("GLOBAL_MAT", StringType(), True),
            StructField("NTGEW", DoubleType(), True),
            StructField("MTART", StringType(), True),
            StructField("BISMT", StringType(), True),
            StructField("LVORM", StringType(), True),
        ]
    )

    df = spark.createDataFrame([], schema)
    result = prep_sap_general_material_data(df, "GLOBAL_MAT")

    assert result.count() == 0
    assert set(result.columns) == {"MATNR", "GLOBAL_MATERIAL_NUMBER", "NTGEW", "MTART"}


def test_prep_sap_general_material_data_invalid_inputs(spark: SparkSession):
    """Test prep_sap_general_material_data with invalid inputs."""
    with pytest.raises(TypeError, match="Invalid input DataFrame"):
        prep_sap_general_material_data(None, "GLOBAL_MAT")

    with pytest.raises(TypeError, match="Invalid input DataFrame"):
        prep_sap_general_material_data("not a dataframe", "GLOBAL_MAT")

    with pytest.raises(TypeError, match="Invalid input column name"):
        prep_sap_general_material_data(spark.createDataFrame([], StructType([])), None)


def test_prep_sap_general_material_data_column_renaming(spark: SparkSession):
    """Test prep_sap_general_material_data with column renaming."""
    schema = StructType(
        [
            StructField("MATNR", StringType(), True),
            StructField("CUSTOM_GLOBAL", StringType(), True),
            StructField("NTGEW", DoubleType(), True),
            StructField("MTART", StringType(), True),
            StructField("BISMT", StringType(), True),
            StructField("LVORM", StringType(), True),
        ]
    )

    data = [("MAT1", "GMAT1", 10.5, "TYPE1", None, None)]

    df = spark.createDataFrame(data, schema)
    result = prep_sap_general_material_data(df, "CUSTOM_GLOBAL")

    assert "GLOBAL_MATERIAL_NUMBER" in result.columns
    assert result.first()["GLOBAL_MATERIAL_NUMBER"] == "GMAT1"
