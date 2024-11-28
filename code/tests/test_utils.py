"""Tests for utils module."""

import pytest
from modules.utils import (check_columns_unique, create_spark_session,
                           mask_sensitive_columns, read_csv_file)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertSchemaEqual


@pytest.fixture
def sample_data(spark: SparkSession) -> DataFrame:
    """Fixture to provide sample data for testing.
    Args:
        spark (SparkSession): Spark session object.

    Returns:
        DataFrame: Spark DataFrame object.
    """
    data = [
        {"name": "Alice", "email": "alice@example.com", "phone": "1234567890"},
        {"name": "Bob", "email": "bob@example.com", "phone": "0987654321"},
        {"name": "Charlie", "email": None, "phone": None},
    ]
    schema = ["name", "email", "phone"]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def duplicate_data(spark: SparkSession) -> DataFrame:
    """Fixture to provide data with duplicates.
    Args:
        spark (SparkSession): Spark session object.
    Returns:
        DataFrame: Spark DataFrame object.
    """
    data = [
        {"MATNR": "MAT1", "AUFNR": "ORD1", "SOURCE_SYSTEM_ERP": "SYS1"},
        {"MATNR": "MAT1", "AUFNR": "ORD2", "SOURCE_SYSTEM_ERP": "SYS1"},  # Duplicate MATNR
        {"MATNR": "MAT3", "AUFNR": "ORD3", "SOURCE_SYSTEM_ERP": "SYS2"},
    ]
    return spark.createDataFrame(data)


def test_create_spark_session():
    """Test create_spark_session function."""
    spark = create_spark_session("test")

    assert spark is not None, "Spark session object is None"
    assert spark.conf.get("spark.app.name") == "test", "Spark application name is incorrect"


def test_read_csv_file_with_inferred_schema(spark: SparkSession):
    """Test read_csv_file function.

    Args:
        spark (SparkSession): Spark session object.
    """
    # Test reading a CSV file with inferred schema

    # CSV file content
    # name,age
    # Alice,25
    # Bob,30
    # Charlie,35

    file_directory = "data/test/test_data.csv"
    df = read_csv_file(spark, file_directory, infer_schema=True)

    assert df is not None, "DataFrame is None"
    assert df.count() == 3, "DataFrame count is incorrect"
    assert df.columns == ["name", "age"], "DataFrame columns are incorrect"


def test_read_csv_file_with_specified_schema(spark: SparkSession):
    """Test read_csv_file function with specified schema.

    Args:
        spark (SparkSession): Spark session object.
    """
    # Test reading a CSV file with specified schema

    # CSV file content
    # name,age
    # Alice,25
    # Bob,30
    # Charlie,35

    file_directory = "data/test/test_data.csv"

    # Define schema
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    # Read CSV file with specified schema
    df = read_csv_file(spark, file_directory, infer_schema=False, schema=schema)

    # Assert results
    assert df is not None
    assert df.count() == 3
    assert df.columns == ["name", "age"]
    assertSchemaEqual(df.schema, schema)


def test_mask_sensitive_columns(spark, sample_data):
    """Test masking of sensitive columns."""
    sensitive_columns = ["email", "phone"]
    masked_df = mask_sensitive_columns(sample_data, sensitive_columns)

    # Check that sensitive columns are masked
    assert masked_df.select("email").collect()[0][0] != "alice@example.com", "Email should be masked"
    assert masked_df.select("phone").collect()[0][0] != "1234567890", "Phone should not be masked"


def test_mask_sensitive_columns_missing_column(sample_data):
    """Test that a ValueError is raised if a sensitive column is not present in the DataFrame."""
    sensitive_columns = ["non_existent_column"]
    with pytest.raises(ValueError, match="Column 'non_existent_column' not found in DataFrame."):
        mask_sensitive_columns(sample_data, sensitive_columns)


def test_mask_sensitive_columns_with_null_values(sample_data):
    """Test that null values in sensitive columns are not masked."""
    sensitive_columns = ["email", "phone"]
    masked_df = mask_sensitive_columns(sample_data, sensitive_columns)

    # Check that null values are not masked
    for col in sensitive_columns:
        null_count = sample_data.filter(F.col(col).isNull()).count()
        masked_null_count = masked_df.filter(F.col(col).isNull()).count()
        assert null_count == masked_null_count, f"Null values in column '{col}' should not be masked"


def test_columns_unique_all_unique(sample_data):
    """Test that no error is raised when all columns are unique."""
    try:
        check_columns_unique(sample_data, ["name", "email", "phone"])
    except ValueError as e:
        pytest.fail(f"Unexpected ValueError raised: {e}")


def test_column_not_unique(duplicate_data):
    """Test that a ValueError is raised for non-unique columns."""
    with pytest.raises(ValueError, match="Column 'MATNR' is not unique."):
        check_columns_unique(duplicate_data, ["MATNR"])


def test_missing_column(sample_data):
    """Test that a ValueError is raised for missing columns."""
    with pytest.raises(ValueError, match="Column 'NON_EXISTENT' is not present in the DataFrame."):
        check_columns_unique(sample_data, ["NON_EXISTENT"])


def test_multiple_columns_mixed(duplicate_data):
    """Test a combination of valid and invalid column uniqueness."""
    with pytest.raises(ValueError, match="Column 'MATNR' is not unique."):
        check_columns_unique(duplicate_data, ["MATNR", "AUFNR"])


def test_empty_column_list(sample_data):
    """Test behavior when the column list is empty."""
    try:
        check_columns_unique(sample_data, [])
    except ValueError as e:
        pytest.fail(f"Unexpected ValueError raised for empty column list: {e}")
