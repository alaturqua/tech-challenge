import pytest
from modules.local_material import prep_plant_and_branches
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual


def test_prep_plant_and_branches_valid_input(spark: SparkSession):
    """Test prep_plant_and_branches with valid input data."""
    data = [
        ("100", "PLANT1", "VAL1", "Plant 1"),
        ("100", "PLANT2", "VAL2", "Plant 2"),
    ]
    schema = "MANDT STRING, WERKS STRING, BWKEY STRING, NAME1 STRING"
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [
        ("100", "PLANT1", "VAL1", "Plant 1"),
        ("100", "PLANT2", "VAL2", "Plant 2"),
    ]
    expected_schema = "MANDT STRING, WERKS STRING, BWKEY STRING, NAME1 STRING"
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_plant_and_branches(input_df)

    assertDataFrameEqual(result_df, expected_df), "DataFrames are not equal"


def test_prep_plant_and_branches_invalid_input():
    """Test prep_plant_and_branches with invalid input type."""
    with pytest.raises(TypeError, match="df must be a DataFrame"):
        prep_plant_and_branches(None)


def test_prep_plant_and_branches_missing_columns(spark: SparkSession):
    """Test prep_plant_and_branches with missing columns in input DataFrame."""
    data = [
        ("100", "PLANT1", "VAL1"),
        ("100", "PLANT2", "VAL2"),
    ]
    schema = "MANDT STRING, WERKS STRING, BWKEY STRING"
    input_df = spark.createDataFrame(data, schema=schema)

    with pytest.raises(Exception):
        prep_plant_and_branches(input_df)

