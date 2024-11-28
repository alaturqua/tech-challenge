import pytest
from modules.local_material import prep_material_valuation
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual


def test_prep_material_valuation_valid_input(spark: SparkSession):
    """Test prep_material_valuation with valid input data.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        ("100", "10000001", "1000", "S", 10.0, 20.0, 1, "3000", None, None, "20240101"),
        ("100", "10000001", "1000", "S", 15.0, 25.0, 1, "3000", None, None, "20240102"),
        ("100", "10000002", "2000", "V", 5.0, 10.0, 1, "4000", None, None, "20240101"),
        ("100", "10000002", "2000", "V", 7.0, 12.0, 1, "4000", None, None, "20240102"),
    ]
    schema = """
        MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING,
        VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING,
        LVORM STRING, BWTAR STRING, LAEPR STRING
    """
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [
        ("100", "10000001", "1000", "S", 15.0, 25.0, 1, "3000"),
        ("100", "10000002", "2000", "V", 7.0, 12.0, 1, "4000"),
    ]
    expected_schema = (
        "MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING, VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING"
    )
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_material_valuation(input_df)
    assertDataFrameEqual(result_df, expected_df)


def test_prep_material_valuation_empty_df(spark: SparkSession):
    """Test prep_material_valuation with empty DataFrame.

    Args:
        spark (SparkSession): Spark session object.
    """
    schema = """
        MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING,
        VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING,
        LVORM STRING, BWTAR STRING, LAEPR STRING
    """
    empty_df = spark.createDataFrame([], schema=schema)
    result_df = prep_material_valuation(empty_df)
    assert result_df.count() == 0


def test_prep_material_valuation_invalid_input():
    """Test prep_material_valuation with invalid input type."""
    with pytest.raises(TypeError, match="df must be a DataFrame"):
        prep_material_valuation(None)


def test_prep_material_valuation_lvorm_filtering(spark: SparkSession):
    """Test prep_material_valuation with LVORM filtering.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        ("100", "10000001", "1000", "S", 10.0, 20.0, 1, "3000", None, None, "20240101"),
        ("100", "10000001", "1000", "S", 15.0, 25.0, 1, "3000", "X", None, "20240102"),
    ]
    schema = """
        MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING,
        VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING,
        LVORM STRING, BWTAR STRING, LAEPR STRING
    """
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [
        ("100", "10000001", "1000", "S", 10.0, 20.0, 1, "3000"),
    ]
    expected_schema = (
        "MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING, VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING"
    )
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_material_valuation(input_df)
    assertDataFrameEqual(result_df, expected_df)


def test_prep_material_valuation_bwtar_filtering(spark: SparkSession):
    """Test prep_material_valuation with BWTAR filtering.

    Args:
        spark (SparkSession): Spark session object.

    """
    data = [
        ("100", "10000001", "1000", "S", 10.0, 20.0, 1, "3000", None, None, "20240101"),
        (
            "100",
            "10000001",
            "1000",
            "S",
            15.0,
            25.0,
            1,
            "3000",
            None,
            "001",
            "20240102",
        ),
    ]
    schema = """
        MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING,
        VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING,
        LVORM STRING, BWTAR STRING, LAEPR STRING
    """
    input_df = spark.createDataFrame(data, schema=schema)

    expected_data = [
        ("100", "10000001", "1000", "S", 10.0, 20.0, 1, "3000"),
    ]
    expected_schema = (
        "MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING, VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING"
    )
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_material_valuation(input_df)
    assertDataFrameEqual(result_df, expected_df)


def test_prep_material_valuation_row_numbering(spark: SparkSession):
    """Test row numbering logic based on LAEPR ordering.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        # Same MATNR/BWKEY group with different LAEPR dates
        ("100", "10000001", "1000", "S", 10.0, 20.0, 1, "3000", None, None, "20240101"),
        ("100", "10000001", "1000", "S", 15.0, 25.0, 1, "3000", None, None, "20240102"),
        # Different MATNR/BWKEY group
        ("100", "10000002", "2000", "V", 5.0, 10.0, 1, "4000", None, None, "20240101"),
        ("100", "10000002", "2000", "V", 7.0, 12.0, 1, "4000", None, None, "20240102"),
    ]
    schema = """
        MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING,
        VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING,
        LVORM STRING, BWTAR STRING, LAEPR STRING
    """
    input_df = spark.createDataFrame(data, schema=schema)

    # Should select latest LAEPR for each MATNR/BWKEY group
    expected_data = [
        ("100", "10000001", "1000", "S", 15.0, 25.0, 1, "3000"),
        ("100", "10000002", "2000", "V", 7.0, 12.0, 1, "4000"),
    ]
    expected_schema = """
        MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING,
        VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING
    """
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_material_valuation(input_df)
    assertDataFrameEqual(result_df, expected_df)


def test_prep_material_valuation_deduplication(spark: SparkSession):
    """Test deduplication of identical rows.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        # Duplicate rows with same values
        ("100", "10000001", "1000", "S", 10.0, 20.0, 1, "3000", None, None, "20240101"),
        ("100", "10000001", "1000", "S", 10.0, 20.0, 1, "3000", None, None, "20240101"),
    ]
    schema = """
        MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING,
        VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING,
        LVORM STRING, BWTAR STRING, LAEPR STRING
    """
    input_df = spark.createDataFrame(data, schema=schema)

    # Should remove duplicates
    expected_data = [
        ("100", "10000001", "1000", "S", 10.0, 20.0, 1, "3000"),
    ]
    expected_schema = """
        MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING,
        VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING
    """
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_material_valuation(input_df)
    assertDataFrameEqual(result_df, expected_df)


def test_prep_material_valuation_complex_scenario(spark: SparkSession):
    """Test combination of filtering, row numbering and deduplication.

    Args:
        spark (SparkSession): Spark session object.
    """
    data = [
        # Group 1: Valid rows with different LAEPR
        ("100", "10000001", "1000", "S", 10.0, 20.0, 1, "3000", None, None, "20240101"),
        ("100", "10000001", "1000", "S", 15.0, 25.0, 1, "3000", None, None, "20240102"),
        # Group 1: Duplicate of latest record
        ("100", "10000001", "1000", "S", 15.0, 25.0, 1, "3000", None, None, "20240102"),
        # Group 1: Row with LVORM flag (should be filtered)
        ("100", "10000001", "1000", "S", 20.0, 30.0, 1, "3000", "X", None, "20240103"),
        # Group 1: Row with BWTAR (should be filtered)
        (
            "100",
            "10000001",
            "1000",
            "S",
            25.0,
            35.0,
            1,
            "3000",
            None,
            "001",
            "20240104",
        ),
    ]
    schema = """
        MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING,
        VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING,
        LVORM STRING, BWTAR STRING, LAEPR STRING
    """
    input_df = spark.createDataFrame(data, schema=schema)

    # Should select only the latest valid record
    expected_data = [
        ("100", "10000001", "1000", "S", 15.0, 25.0, 1, "3000"),
    ]
    expected_schema = """
        MANDT STRING, MATNR STRING, BWKEY STRING, VPRSV STRING,
        VERPR DOUBLE, STPRS DOUBLE, PEINH INT, BKLAS STRING
    """
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = prep_material_valuation(input_df)
    assertDataFrameEqual(result_df, expected_df)
