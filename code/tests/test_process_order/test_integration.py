import pytest
from modules.process_order import integration
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


@pytest.fixture
def sample_data(spark: SparkSession) -> dict:
    """Fixture to create sample data for tests.

    Args:
        spark (SparkSession): The Spark session.

    Returns:
        dict[str, DataFrame]: Sample DataFrames for testing.
    """
    sap_afko = spark.createDataFrame(
        data=[("1000", "OBJ1", "2024-12-31")],
        schema=StructType(
            [
                StructField("AUFNR", StringType(), True),
                StructField("OBJNR", StringType(), True),
                StructField("GLTRP", StringType(), True),
            ]
        ),
    )
    sap_afpo = spark.createDataFrame(
        data=[("1000", "MATERIAL1")],
        schema=StructType(
            [
                StructField("AUFNR", StringType(), True),
                StructField("MATNR", StringType(), True),
            ]
        ),
    )
    sap_aufk = spark.createDataFrame(
        data=[("1000", None)],
        schema=StructType(
            [
                StructField("AUFNR", StringType(), True),
                StructField("ZZGLTRP_ORIG", StringType(), True),
            ]
        ),
    )
    sap_mara = spark.createDataFrame(
        data=[("MATERIAL1", "Sample Material")],
        schema=StructType(
            [
                StructField("MATNR", StringType(), True),
                StructField("DESCRIPTION", StringType(), True),
            ]
        ),
    )
    sap_cdpos = spark.createDataFrame(
        data=[("OBJ1", "Change1")],
        schema=StructType(
            [
                StructField("OBJNR", StringType(), True),
                StructField("CHANGE", StringType(), True),
            ]
        ),
    )
    return {
        "sap_afko": sap_afko,
        "sap_afpo": sap_afpo,
        "sap_aufk": sap_aufk,
        "sap_mara": sap_mara,
        "sap_cdpos": sap_cdpos,
    }


def test_integration_required_dataframes(sample_data):
    """Test `integration` function with all required DataFrames.

    Args:
        spark (SparkSession): The Spark session.
        sample_data (dict): Sample data for testing.
    """
    result = integration(
        sap_afko=sample_data["sap_afko"],
        sap_afpo=sample_data["sap_afpo"],
        sap_aufk=sample_data["sap_aufk"],
        sap_mara=sample_data["sap_mara"],
    )
    assert isinstance(result, DataFrame), "Result should be a DataFrame."
    assert result.count() == 1, "Result should contain 1 row."
    assert "ZZGLTRP_ORIG" in result.columns, "Result should have 'ZZGLTRP_ORIG' column."


def test_integration_with_optional_dataframe(sample_data):
    """Test `integration` function with an optional DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        sample_data (dict): Sample data for testing.
    """
    result = integration(
        sap_afko=sample_data["sap_afko"],
        sap_afpo=sample_data["sap_afpo"],
        sap_aufk=sample_data["sap_aufk"],
        sap_mara=sample_data["sap_mara"],
        sap_cdpos=sample_data["sap_cdpos"],
    )
    assert "CHANGE" in result.columns, "Result should include columns from optional DataFrame."


def test_integration_invalid_input(spark: SparkSession):
    """Test `integration` function with invalid input.

    Args:
        spark (SparkSession): The Spark session.
    """
    with pytest.raises(TypeError, match="Expected DataFrame for sap_afko"):
        integration(
            sap_afko="not_a_dataframe",
            sap_afpo=None,
            sap_aufk=None,
            sap_mara=None,
        )

    with pytest.raises(TypeError, match="Expected DataFrame or None for sap_cdpos"):
        integration(
            sap_afko=spark.createDataFrame([Row(AUFNR="1000")]),
            sap_afpo=spark.createDataFrame([Row(AUFNR="1000")]),
            sap_aufk=spark.createDataFrame([Row(AUFNR="1000")]),
            sap_mara=spark.createDataFrame([Row(AUFNR="1000")]),
            sap_cdpos="not_a_dataframe",
        )
