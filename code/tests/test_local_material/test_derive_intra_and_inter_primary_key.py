import pytest
from modules.local_material import derive_intra_and_inter_primary_key
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def test_derive_primary_keys(spark):
    """Test derive_intra_and_inter_primary_key function."""
    # Setup schema
    schema = StructType(
        [
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("WERKS", StringType(), True),
        ]
    )

    # Test case 1: Normal case
    data = [("SYS1", "MAT1", "PLANT1"), ("SYS2", "MAT2", "PLANT2")]
    input_df = spark.createDataFrame(data, schema)
    result = derive_intra_and_inter_primary_key(input_df)

    # Verify results
    assert "primary_key_intra" in result.columns, "Result should contain primary_key_intra column"
    assert "primary_key_inter" in result.columns, "Result should contain primary_key_inter column"

    first_row = result.first()
    assert first_row["primary_key_intra"] == "MAT1-PLANT1", "Primary key should be derived correctly"
    assert first_row["primary_key_inter"] == "SYS1-MAT1-PLANT1", "Primary key should be derived correctly"


def test_derive_primary_keys_with_nulls(spark):
    """Test derive_intra_and_inter_primary_key function with null values."""
    # Setup schema
    schema = StructType(
        [
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("WERKS", StringType(), True),
        ]
    )

    # Test with various null combinations
    data = [("SYS1", None, "PLANT1"), ("SYS2", "MAT2", None), (None, "MAT3", "PLANT3")]
    input_df = spark.createDataFrame(data, schema)
    result = derive_intra_and_inter_primary_key(input_df)

    # Check results row by row
    rows = result.collect()

    # First row: null MATNR
    assert rows[0]["primary_key_intra"] == "-PLANT1", "Primary key should be derived correctly"
    assert rows[0]["primary_key_inter"] == "SYS1--PLANT1", "Primary key should be derived correctly"

    # Second row: null WERKS
    assert rows[1]["primary_key_intra"] == "MAT2-", "Primary key should be derived correctly"
    assert rows[1]["primary_key_inter"] == "SYS2-MAT2-", "Primary key should be derived correctly"

    # Third row: null SOURCE_SYSTEM_ERP
    assert rows[2]["primary_key_intra"] == "MAT3-PLANT3", "Primary key should be derived correctly"
    assert rows[2]["primary_key_inter"] == "-MAT3-PLANT3", "Primary key should be derived correctly"


def test_derive_primary_keys_empty_df(spark):
    """Test derive_intra_and_inter_primary_key function with an empty DataFrame."""
    # Test empty DataFrame
    schema = StructType(
        [
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("WERKS", StringType(), True),
        ]
    )
    empty_df = spark.createDataFrame([], schema)
    result = derive_intra_and_inter_primary_key(empty_df)
    assert result.count() == 0, "Result DataFrame should be empty"


def test_derive_primary_keys_missing_columns(spark):
    """Test derive_intra_and_inter_primary_key function with missing required columns."""
    # Test missing required columns
    invalid_schema = StructType(
        [StructField("SOURCE_SYSTEM_ERP", StringType(), True), StructField("MATNR", StringType(), True)]
    )
    invalid_df = spark.createDataFrame([("SYS1", "MAT1")], invalid_schema)

    with pytest.raises(Exception):
        derive_intra_and_inter_primary_key(invalid_df)
