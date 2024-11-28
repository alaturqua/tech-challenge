import pytest
from modules.local_material import post_prep_local_material
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def test_post_prep_local_material(spark: SparkSession):
    """Test post-preparation for Local Material data."""
    # Setup test schema
    schema = StructType(
        [
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("WERKS", StringType(), True),
            StructField("NAME1", StringType(), True),
            StructField("GLOBAL_MATERIAL_NUMBER", StringType(), True),
        ]
    )

    # Test case 1: Normal case
    test_data = [("SYS1", "MAT1", "PLANT1", "Plant Name 1", "GMAT1"), ("SYS1", "MAT2", "PLANT2", "Plant Name 2", None)]
    input_df = spark.createDataFrame(test_data, schema)
    result = post_prep_local_material(input_df)

    # Verify results
    assert result.count() == 2
    assert "mtl_plant_emd" in result.columns
    assert "global_mtl_id" in result.columns
    assert "primary_key_intra" in result.columns
    assert "primary_key_inter" in result.columns

    # Test case 2: Duplicate handling
    dup_data = [
        ("SYS1", "MAT1", "PLANT1", "Plant Name 1", "GMAT1"),
        ("SYS1", "MAT1", "PLANT1", "Plant Name 1", "GMAT1"),
    ]
    dup_df = spark.createDataFrame(dup_data, schema)
    dup_result = post_prep_local_material(dup_df)
    assert dup_result.count() == 1

    # Test case 3: Null values
    null_data = [("SYS1", "MAT1", "PLANT1", None, None)]
    null_df = spark.createDataFrame(null_data, schema)
    null_result = post_prep_local_material(null_df)
    assert null_result.filter(F.col("global_mtl_id") == "MAT1").count() == 1

    # Test case 4: Empty DataFrame
    empty_df = spark.createDataFrame([], schema)
    empty_result = post_prep_local_material(empty_df)
    assert empty_result.count() == 0

    # Test case 5: Column validation
    invalid_schema = StructType(
        [StructField("SOURCE_SYSTEM_ERP", StringType(), True), StructField("MATNR", StringType(), True)]
    )
    invalid_df = spark.createDataFrame([], invalid_schema)
    with pytest.raises(Exception):
        post_prep_local_material(invalid_df)
