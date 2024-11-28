import pytest
from modules.local_material import prep_plant_data_for_material
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual


def test_prep_plant_data_for_material_valid_input(spark: SparkSession):
    """Test with valid input and default parameters.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        ("SYS1", "10000001", "PLANT1", None),
        ("SYS1", "10000002", "PLANT2", None),
    ]
    schema = "SOURCE_SYSTEM_ERP STRING, MATNR STRING, WERKS STRING, LVORM STRING"
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [
        ("SYS1", "10000001", "PLANT1"),
        ("SYS1", "10000002", "PLANT2"),
    ]
    expected_schema = "SOURCE_SYSTEM_ERP STRING, MATNR STRING, WERKS STRING"
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_plant_data_for_material(
        input_df, check_deletion_flag_is_null=True, drop_duplicate_records=False
    )

    assertDataFrameEqual(result_df, expected_df)


def test_prep_plant_data_for_material_invalid_input(spark: SparkSession):
    """Test with invalid input types.

    Args:
        spark (SparkSession): Spark session object.
    """
    with pytest.raises(TypeError, match="df must be a DataFrame"):
        prep_plant_data_for_material(
            None, check_deletion_flag_is_null=True, drop_duplicate_records=False
        )

    with pytest.raises(
        TypeError, match="check_deletion_flag_is_null must be a boolean"
    ):
        prep_plant_data_for_material(
            spark.createDataFrame([], "SOURCE_SYSTEM_ERP STRING"),
            check_deletion_flag_is_null="True",
            drop_duplicate_records=False,
        )


def test_prep_plant_data_for_material_deletion_flag(spark: SparkSession):
    """Test deletion flag filtering.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        ("SYS1", "10000001", "PLANT1", None),
        ("SYS1", "10000002", "PLANT2", "X"),
    ]
    schema = "SOURCE_SYSTEM_ERP STRING, MATNR STRING, WERKS STRING, LVORM STRING"
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [("SYS1", "10000001", "PLANT1")]
    expected_schema = "SOURCE_SYSTEM_ERP STRING, MATNR STRING, WERKS STRING"
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_plant_data_for_material(
        input_df, check_deletion_flag_is_null=True, drop_duplicate_records=False
    )

    assertDataFrameEqual(result_df, expected_df)


def test_prep_plant_data_for_material_duplicates(spark: SparkSession):
    """Test duplicate record handling.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        ("SYS1", "10000001", "PLANT1", None),
        ("SYS1", "10000001", "PLANT1", None),
    ]
    schema = "SOURCE_SYSTEM_ERP STRING, MATNR STRING, WERKS STRING, LVORM STRING"
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [("SYS1", "10000001", "PLANT1")]
    expected_schema = "SOURCE_SYSTEM_ERP STRING, MATNR STRING, WERKS STRING"
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_plant_data_for_material(
        input_df, check_deletion_flag_is_null=True, drop_duplicate_records=True
    )

    assertDataFrameEqual(result_df, expected_df)


def test_prep_plant_data_for_material_additional_fields(spark: SparkSession):
    """Test additional fields handling.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        ("SYS1", "10000001", "PLANT1", None, "CAT1"),
        ("SYS1", "10000002", "PLANT2", None, "CAT2"),
    ]
    schema = """
        SOURCE_SYSTEM_ERP STRING, MATNR STRING, WERKS STRING,
        LVORM STRING, CATEGORY STRING
    """
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [
        ("SYS1", "10000001", "PLANT1", "CAT1"),
        ("SYS1", "10000002", "PLANT2", "CAT2"),
    ]
    expected_schema = """
        SOURCE_SYSTEM_ERP STRING, MATNR STRING, WERKS STRING,
        CATEGORY STRING
    """
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_plant_data_for_material(
        input_df,
        check_deletion_flag_is_null=True,
        drop_duplicate_records=False,
        additional_fields=["CATEGORY"],
    )

    assertDataFrameEqual(result_df, expected_df)
