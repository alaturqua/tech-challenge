from modules.local_material import integration
from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)
from pyspark.testing import assertDataFrameEqual


def test_integration_happy_path(spark: SparkSession):
    """Test integration with complete matching data."""
    # Setup test data
    sap_mara_data = [("100", "MAT1", "UOM1", "GMN1")]
    sap_mbew_data = [("100", "MAT1", "VAL1", "PC1", 10.0, 20.0, 1, "VC1")]
    sap_marc_data = [("ERP1", "MAT1", "PLANT1")]
    sap_t001k_data = [("100", "VAL1", "COMP1")]
    sap_t001w_data = [("100", "PLANT1", "VAL1", "Plant Name")]
    sap_t001_data = [("100", "COMP1", "USD")]

    # Create DataFrames
    sap_mara = spark.createDataFrame(sap_mara_data, ["MANDT", "MATNR", "MEINS", "GLOBAL_MATERIAL_NUMBER"])
    sap_mbew = spark.createDataFrame(
        sap_mbew_data, ["MANDT", "MATNR", "BWKEY", "VPRSV", "VERPR", "STPRS", "PEINH", "BKLAS"]
    )
    sap_marc = spark.createDataFrame(sap_marc_data, ["SOURCE_SYSTEM_ERP", "MATNR", "WERKS"])
    sap_t001k = spark.createDataFrame(sap_t001k_data, ["MANDT", "BWKEY", "BUKRS"])
    sap_t001w = spark.createDataFrame(sap_t001w_data, ["MANDT", "WERKS", "BWKEY", "NAME1"])
    sap_t001 = spark.createDataFrame(sap_t001_data, ["MANDT", "BUKRS", "WAERS"])

    # Execute integration
    result_df = integration(sap_mara, sap_mbew, sap_marc, sap_t001k, sap_t001w, sap_t001)

    # Expected data
    expected_data = [
        (
            "ERP1",
            "MAT1",
            "PLANT1",
            "100",
            "UOM1",
            "GMN1",
            "VAL1",
            "Plant Name",
            "PC1",
            10.0,
            20.0,
            1,
            "VC1",
            "COMP1",
            "USD",
        )
    ]

    # Create expected DataFrame with explicit column order
    columns = [
        "SOURCE_SYSTEM_ERP",
        "MATNR",
        "WERKS",
        "MANDT",
        "MEINS",
        "GLOBAL_MATERIAL_NUMBER",
        "BWKEY",
        "NAME1",
        "VPRSV",
        "VERPR",
        "STPRS",
        "PEINH",
        "BKLAS",
        "BUKRS",
        "WAERS",
    ]

    expected_df = spark.createDataFrame(expected_data, columns)

    # Ensure result has same column order before comparison
    result_df_ordered = result_df.select(*columns)

    assertDataFrameEqual(result_df_ordered, expected_df)


def test_integration_empty_dataframes(spark):
    """Test integration with empty DataFrames."""
    # Define schemas
    sap_mara_schema = StructType(
        [
            StructField("MANDT", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("MEINS", StringType(), True),
            StructField("GLOBAL_MATERIAL_NUMBER", StringType(), True),
        ]
    )

    sap_mbew_schema = StructType(
        [
            StructField("MANDT", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("BWKEY", StringType(), True),
            StructField("VPRSV", StringType(), True),
            StructField("VERPR", DoubleType(), True),
            StructField("STPRS", DoubleType(), True),
            StructField("PEINH", LongType(), True),
            StructField("BKLAS", StringType(), True),
        ]
    )

    sap_marc_schema = StructType(
        [
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("WERKS", StringType(), True),
        ]
    )

    sap_t001k_schema = StructType(
        [
            StructField("MANDT", StringType(), True),
            StructField("BWKEY", StringType(), True),
            StructField("BUKRS", StringType(), True),
        ]
    )

    sap_t001w_schema = StructType(
        [
            StructField("MANDT", StringType(), True),
            StructField("WERKS", StringType(), True),
            StructField("BWKEY", StringType(), True),
            StructField("NAME1", StringType(), True),
        ]
    )

    sap_t001_schema = StructType(
        [
            StructField("MANDT", StringType(), True),
            StructField("BUKRS", StringType(), True),
            StructField("WAERS", StringType(), True),
        ]
    )

    # Create empty DataFrames with schemas
    sap_mara = spark.createDataFrame([], sap_mara_schema)
    sap_mbew = spark.createDataFrame([], sap_mbew_schema)
    sap_marc = spark.createDataFrame([], sap_marc_schema)
    sap_t001k = spark.createDataFrame([], sap_t001k_schema)
    sap_t001w = spark.createDataFrame([], sap_t001w_schema)
    sap_t001 = spark.createDataFrame([], sap_t001_schema)

    # Execute integration
    result_df = integration(sap_mara, sap_mbew, sap_marc, sap_t001k, sap_t001w, sap_t001)

    # Verify result is empty DataFrame
    assert result_df.count() == 0


def test_integration_null_values(spark):
    """Test integration with null values."""
    # Define schemas with explicit types
    sap_mara_schema = StructType(
        [
            StructField("MANDT", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("MEINS", StringType(), True),
            StructField("GLOBAL_MATERIAL_NUMBER", StringType(), True),
        ]
    )

    sap_mbew_schema = StructType(
        [
            StructField("MANDT", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("BWKEY", StringType(), True),
            StructField("VPRSV", StringType(), True),
            StructField("VERPR", DoubleType(), True),
            StructField("STPRS", DoubleType(), True),
            StructField("PEINH", LongType(), True),
            StructField("BKLAS", StringType(), True),
        ]
    )

    sap_marc_schema = StructType(
        [
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("WERKS", StringType(), True),
        ]
    )

    sap_t001k_schema = StructType(
        [
            StructField("MANDT", StringType(), True),
            StructField("BWKEY", StringType(), True),
            StructField("BUKRS", StringType(), True),
        ]
    )

    sap_t001w_schema = StructType(
        [
            StructField("MANDT", StringType(), True),
            StructField("WERKS", StringType(), True),
            StructField("BWKEY", StringType(), True),
            StructField("NAME1", StringType(), True),
        ]
    )

    sap_t001_schema = StructType(
        [
            StructField("MANDT", StringType(), True),
            StructField("BUKRS", StringType(), True),
            StructField("WAERS", StringType(), True),
        ]
    )

    # Test data with nulls
    sap_mara = spark.createDataFrame([("100", "MAT1", None, None)], sap_mara_schema)
    sap_mbew = spark.createDataFrame([("100", "MAT1", "VAL1", None, None, None, None, None)], sap_mbew_schema)
    sap_marc = spark.createDataFrame([("ERP1", "MAT1", "PLANT1")], sap_marc_schema)
    sap_t001k = spark.createDataFrame([("100", "VAL1", None)], sap_t001k_schema)
    sap_t001w = spark.createDataFrame([("100", "PLANT1", "VAL1", None)], sap_t001w_schema)
    sap_t001 = spark.createDataFrame([("100", None, None)], sap_t001_schema)

    # Execute integration
    result_df = integration(sap_mara, sap_mbew, sap_marc, sap_t001k, sap_t001w, sap_t001)

    # Verify results
    assert result_df.count() > 0, "Result DataFrame should not be empty"

    # Get first row
    first_row = result_df.first()
    assert first_row is not None, "First row should not be None"

    # Verify null values in specific columns
    assert first_row["MEINS"] is None, "MEINS column should be None"
    assert first_row["GLOBAL_MATERIAL_NUMBER"] is None, "GLOBAL_MATERIAL_NUMBER column should be None"
