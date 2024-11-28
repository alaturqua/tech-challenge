import pytest
from modules.local_material import prep_general_material_data
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual


def test_prep_general_material_data_valid_input(spark: SparkSession):
    """Test prep_general_material_data with valid input data.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        (
            "100",
            "10000001",
            "EA",
            "10000001",
            None,
            None,
        ),  # Should pass - null BISMT, null LVORM
        (
            "100",
            "10000002",
            "EA",
            "10000002",
            "ARCHIVE",
            None,
        ),  # Should fail - ARCHIVE in BISMT
        (
            "100",
            "10000003",
            "EA",
            "10000003",
            None,
            "X",
        ),  # Should fail - non-empty LVORM
    ]
    schema = "MANDT STRING, MATNR STRING, MEINS STRING, GLOBAL_MATERIAL_NUMBER STRING, BISMT STRING, LVORM STRING"
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [("100", "10000001", "EA", "10000001")]
    expected_schema = "MANDT STRING, MATNR STRING, MEINS STRING, GLOBAL_MATERIAL_NUMBER STRING"
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_general_material_data(
        input_df,
        col_mara_global_material_number="GLOBAL_MATERIAL_NUMBER",
        check_old_material_number_is_valid=True,
        check_material_is_not_deleted=True,
    )

    assertDataFrameEqual(result_df, expected_df)


def test_prep_general_material_data_empty_df(spark: SparkSession):
    """Test prep_general_material_data with empty DataFrame.

    Args:
        spark (SparkSession): Spark session object.
    """
    schema = "MANDT STRING, MATNR STRING, MEINS STRING, GLOBAL_MATERIAL_NUMBER STRING, BISMT STRING, LVORM STRING"
    empty_df = spark.createDataFrame([], schema=schema)

    result_df = prep_general_material_data(
        empty_df,
        col_mara_global_material_number="GLOBAL_MATERIAL_NUMBER",
        check_old_material_number_is_valid=True,
        check_material_is_not_deleted=True,
    )

    assert result_df.count() == 0


def test_prep_general_material_data_invalid_input():
    """Test prep_general_material_data with invalid input type.

    This test should raise a TypeError.
    """
    with pytest.raises(TypeError, match="df must be a DataFrame"):
        prep_general_material_data(
            None,
            col_mara_global_material_number="GLOBAL_MATERIAL_NUMBER",
            check_old_material_number_is_valid=True,
            check_material_is_not_deleted=True,
        )


def test_prep_general_material_data_bismt_filtering(spark: SparkSession):
    """Test prep_general_material_data with BISMT filtering.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        ("100", "10000001", "EA", "10000001", None, None),
        ("100", "10000002", "EA", "10000002", "ARCHIVE", None),
        ("100", "10000003", "EA", "10000003", "DUPLICATE", None),
        ("100", "10000004", "EA", "10000004", "RENUMBERED", None),
        ("100", "10000005", "EA", "10000005", "VALID", None),
    ]
    schema = "MANDT STRING, MATNR STRING, MEINS STRING, GLOBAL_MATERIAL_NUMBER STRING, BISMT STRING, LVORM STRING"
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [
        ("100", "10000001", "EA", "10000001"),
        ("100", "10000005", "EA", "10000005"),
    ]
    expected_schema = "MANDT STRING, MATNR STRING, MEINS STRING, GLOBAL_MATERIAL_NUMBER STRING"
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_general_material_data(
        input_df,
        col_mara_global_material_number="GLOBAL_MATERIAL_NUMBER",
        check_old_material_number_is_valid=True,
        check_material_is_not_deleted=True,
    )

    assertDataFrameEqual(result_df, expected_df)


def test_prep_general_material_data_lvorm_filtering(spark: SparkSession):
    """Test prep_general_material_data with LVORM filtering.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        ("100", "10000001", "EA", "10000001", None, None),
        ("100", "10000002", "EA", "10000002", None, "X"),
        ("100", "10000003", "EA", "10000003", None, ""),
    ]
    schema = "MANDT STRING, MATNR STRING, MEINS STRING, GLOBAL_MATERIAL_NUMBER STRING, BISMT STRING, LVORM STRING"
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [
        ("100", "10000001", "EA", "10000001"),
        ("100", "10000003", "EA", "10000003"),
    ]
    expected_schema = "MANDT STRING, MATNR STRING, MEINS STRING, GLOBAL_MATERIAL_NUMBER STRING"
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_general_material_data(
        input_df,
        col_mara_global_material_number="GLOBAL_MATERIAL_NUMBER",
        check_old_material_number_is_valid=True,
        check_material_is_not_deleted=True,
    )

    assertDataFrameEqual(result_df, expected_df)
