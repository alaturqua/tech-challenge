"""Main script to run the technical challenge.

Exercise Overview:
You will work with data from two SAP systems that have similar data sources.
Your task is to process and integrate this data to provide a unified view for supply chain insights.

The exercise involves:
* Processing Local Material data.Processing Process Order data.
* Ensuring both datasets have the same schema for harmonization across systems.
* Writing modular, reusable code with proper documentation.
* Following best-in-class principles for flexibility and maintainability.

Note: You will create two Python scripts (local_material.py and process_order.py)
for each system, i.e. in total of four scripts (2 systems per modeled entity/event).

General Instructions
Work on both SAP systems:
* Perform all the steps for both systems to ensure consistency
* Enable and accomplish data harmonization through a common data model

Focus on data fields and transformations:
* Pay attention to the required fields and the transformations applied to them.
* Document your code: Include comments explaining why certain modules and functions are used.
* Follow best practices: Write modular code, handle exceptions, and ensure reusability.


Detailed instructions see attached PDF

"""

from pathlib import Path

from modules.local_material import integration as integration_local_material
from modules.local_material import (
    post_prep_local_material,
    prep_company_codes,
    prep_general_material_data,
    prep_material_valuation,
    prep_plant_and_branches,
    prep_plant_data_for_material,
    prep_valuation_data,
)
from modules.process_order import integration as integration_process_order
from modules.process_order import (
    post_prep_process_order,
    prep_sap_general_material_data,
    prep_sap_order_header_data,
    prep_sap_order_item,
    prep_sap_order_master_data,
)
from modules.utils import check_columns_unique, create_spark_session, get_logger, mask_sensitive_columns, read_csv_file
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.testing.utils import assertSchemaEqual

logger = get_logger(__name__)

parent_dir_name = Path(__file__).resolve().parents[1]
logger.info(f"Parent directory: {parent_dir_name}")

spark = create_spark_session("technical-challenge")


def process_local_material() -> DataFrame:
    """Process Local Material data from two SAP systems."""
    logger.info("Processing Local Material data")
    logger.info("Loading data from system_1")

    # Load data
    pre_mara = read_csv_file(spark, parent_dir_name / "data" / "system_1" / "PRE_MARA.csv")
    pre_mbew = read_csv_file(spark, parent_dir_name / "data" / "system_1" / "PRE_MBEW.csv")
    pre_marc = read_csv_file(spark, parent_dir_name / "data" / "system_1" / "PRE_MARC.csv")
    pre_t001k = read_csv_file(spark, parent_dir_name / "data" / "system_1" / "PRE_T001K.csv")
    pre_t001w = read_csv_file(spark, parent_dir_name / "data" / "system_1" / "PRE_T001W.csv")
    pre_t001 = read_csv_file(spark, parent_dir_name / "data" / "system_1" / "PRE_T001.csv")

    # Process data
    logger.info("Processing data from system_1")
    pre_mara = prep_general_material_data(
        pre_mara,
        col_mara_global_material_number="ZZMDGM",
        check_old_material_number_is_valid=True,
        check_material_is_not_deleted=True,
    )
    pre_mbew = prep_material_valuation(pre_mbew)
    pre_marc = prep_plant_data_for_material(pre_marc, check_deletion_flag_is_null=True, drop_duplicate_records=True)
    pre_t001k = prep_valuation_data(pre_t001k)
    pre_t001w = prep_plant_and_branches(pre_t001w)
    pre_t001 = prep_company_codes(pre_t001)

    logger.info("Loading data from system_2")
    # Load data
    prd_mara = read_csv_file(spark, parent_dir_name / "data" / "system_2" / "PRD_MARA.csv")
    prd_mbew = read_csv_file(spark, parent_dir_name / "data" / "system_2" / "PRD_MBEW.csv")
    prd_marc = read_csv_file(spark, parent_dir_name / "data" / "system_2" / "PRD_MARC.csv")
    prd_t001k = read_csv_file(spark, parent_dir_name / "data" / "system_2" / "PRD_T001K.csv")
    prd_t001w = read_csv_file(spark, parent_dir_name / "data" / "system_2" / "PRD_T001W.csv")
    prd_t001 = read_csv_file(spark, parent_dir_name / "data" / "system_2" / "PRD_T001.csv")

    # Process data
    logger.info("Processing data from system_2")
    prd_mara = prep_general_material_data(
        prd_mara,
        col_mara_global_material_number="ZZMDGM",
        check_old_material_number_is_valid=True,
        check_material_is_not_deleted=True,
    )
    prd_mbew = prep_material_valuation(prd_mbew)
    prd_marc = prep_plant_data_for_material(prd_marc, check_deletion_flag_is_null=True, drop_duplicate_records=True)
    prd_t001k = prep_valuation_data(prd_t001k)
    prd_t001w = prep_plant_and_branches(prd_t001w)
    prd_t001 = prep_company_codes(prd_t001)

    logger.info("Union data from both systems")
    union_mara = pre_mara.unionByName(prd_mara)
    union_mbew = pre_mbew.unionByName(prd_mbew)
    union_marc = pre_marc.unionByName(prd_marc)
    union_t001k = pre_t001k.unionByName(prd_t001k)
    union_t001w = pre_t001w.unionByName(prd_t001w)
    union_t001 = pre_t001.unionByName(prd_t001)

    # Post-process data
    integrated_data = integration_local_material(
        union_mara, union_mbew, union_marc, union_t001k, union_t001w, union_t001
    )

    local_material = post_prep_local_material(integrated_data)

    schema = StructType(
        [
            StructField("MANDT", StringType(), True),
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("WERKS", StringType(), True),
            StructField("MEINS", StringType(), True),
            StructField("GLOBAL_MATERIAL_NUMBER", StringType(), True),
            StructField("BWKEY", StringType(), True),
            StructField("NAME1", StringType(), True),
            StructField("VPRSV", StringType(), True),
            StructField("VERPR", DoubleType(), True),
            StructField("STPRS", DoubleType(), True),
            StructField("PEINH", DoubleType(), True),
            StructField("BKLAS", StringType(), True),
            StructField("BUKRS", StringType(), True),
            StructField("WAERS", StringType(), True),
            StructField("primary_key_intra", StringType(), False),
            StructField("primary_key_inter", StringType(), False),
            StructField("mtl_plant_emd", StringType(), False),
            StructField("global_mtl_id", StringType(), True),
        ]
    )

    sensitive_columns = [
        "GLOBAL_MATERIAL_NUMBER",  # Global material number
        "NAME1",  # Branch name
        "BUKRS",  # Company code
        "mtl_plant_emd",  # Material plant EMD
        "MATNR",  # Material number
    ]

    local_material = mask_sensitive_columns(local_material, sensitive_columns)
    check_columns_unique(local_material, ["primary_key_intra", "primary_key_inter"])

    logger.info("Validating schema for Local Material data")
    assertSchemaEqual(local_material.schema, schema)

    return local_material


def process_process_order() -> DataFrame:
    """Process Process Order data from two SAP systems."""
    # Load data
    logger.info("Processing Process Order data")
    logger.info("Loading data from system_1")

    pre_afko = read_csv_file(spark, parent_dir_name / "data" / "system_1" / "PRE_AFKO.csv")
    pre_afpo = read_csv_file(spark, parent_dir_name / "data" / "system_1" / "PRE_AFPO.csv")
    pre_aufk = read_csv_file(spark, parent_dir_name / "data" / "system_1" / "PRE_AUFK.csv")
    pre_mara = read_csv_file(spark, parent_dir_name / "data" / "system_1" / "PRE_MARA.csv")

    # Process data
    logger.info("Processing data from system_1")
    pre_afko = prep_sap_order_header_data(pre_afko)
    pre_afpo = prep_sap_order_item(pre_afpo)
    pre_aufk = prep_sap_order_master_data(pre_aufk)
    pre_mara = prep_sap_general_material_data(pre_mara, col_global_material="ZZMDGM")

    logger.info("Loading data from system_2")
    prd_afko = read_csv_file(spark, parent_dir_name / "data" / "system_2" / "PRD_AFKO.csv")
    prd_afpo = read_csv_file(spark, parent_dir_name / "data" / "system_2" / "PRD_AFPO.csv")
    prd_aufk = read_csv_file(spark, parent_dir_name / "data" / "system_2" / "PRD_AUFK.csv")
    prd_mara = read_csv_file(spark, parent_dir_name / "data" / "system_2" / "PRD_MARA.csv")

    # Process data
    logger.info("Processing data from system_2")
    prd_afko = prep_sap_order_header_data(prd_afko)
    prd_afpo = prep_sap_order_item(prd_afpo)
    prd_aufk = prep_sap_order_master_data(prd_aufk)
    prd_mara = prep_sap_general_material_data(prd_mara, col_global_material="ZZMDGM")

    logger.info("Union data from both systems")
    union_afko = pre_afko.unionByName(prd_afko)
    union_afpo = pre_afpo.unionByName(prd_afpo)
    union_aufk = pre_aufk.unionByName(prd_aufk)
    union_mara = pre_mara.unionByName(prd_mara)

    # Post-process data
    integrated_data = integration_process_order(union_afko, union_afpo, union_aufk, union_mara)

    process_order = post_prep_process_order(integrated_data)

    expected_schema = StructType(
        [
            StructField("MATNR", StringType(), True),
            StructField("AUFNR", StringType(), True),
            StructField("SOURCE_SYSTEM_ERP", StringType(), True),
            StructField("MANDT", StringType(), True),
            StructField("GLTRP", DateType(), True),
            StructField("GSTRP", DateType(), True),
            StructField("FTRMS", DateType(), True),
            StructField("GLTRS", DateType(), True),
            StructField("GSTRS", DateType(), True),
            StructField("GSTRI", DateType(), True),
            StructField("GETRI", DateType(), True),
            StructField("GLTRI", DateType(), True),
            StructField("FTRMI", DateType(), True),
            StructField("FTRMP", DateType(), True),
            StructField("DISPO", StringType(), True),
            StructField("FEVOR", StringType(), True),
            StructField("PLGRP", StringType(), True),
            StructField("FHORI", StringType(), True),
            StructField("AUFPL", StringType(), True),
            StructField("start_date", DateType(), True),
            StructField("finish_date", DateType(), True),
            StructField("POSNR", StringType(), True),
            StructField("DWERK", StringType(), True),
            StructField("MEINS", StringType(), True),
            StructField("KDAUF", StringType(), True),
            StructField("KDPOS", StringType(), True),
            StructField("LTRMI", DateType(), True),
            StructField("OBJNR", StringType(), True),
            StructField("ERDAT", DateType(), True),
            StructField("ERNAM", StringType(), True),
            StructField("AUART", StringType(), True),
            StructField("ZZGLTRP_ORIG", DateType(), True),
            StructField("ZZPRO_TEXT", StringType(), True),
            StructField("GLOBAL_MATERIAL_NUMBER", StringType(), True),
            StructField("NTGEW", DoubleType(), True),
            StructField("MTART", StringType(), True),
            StructField("primary_key_intra", StringType(), False),
            StructField("primary_key_inter", StringType(), False),
            StructField("on_time_flag", IntegerType(), True),
            StructField("actual_on_time_deviation", DoubleType(), True),
            StructField("late_delivery_bucket", StringType(), False),
            StructField("mto_vs_mts_flag", StringType(), False),
            StructField("order_finish_timestamp", TimestampType(), True),
            StructField("order_start_timestamp", TimestampType(), True),
        ]
    )

    sensitive_columns = [
        "AUFNR",  # Order number
        "MATNR",  # Material number
        "KDPOS",  # Sales order item
        "KDAUF",  # Sales order
        "ERNAM",  # Created by
        "OBJNR",  # Object number
    ]

    process_order = mask_sensitive_columns(process_order, sensitive_columns)

    check_columns_unique(process_order, ["primary_key_intra", "primary_key_inter"])
    assertSchemaEqual(process_order.schema, expected_schema)
    return process_order


def harmonize_data():
    """Harmonize Local Material and Process Order data from two SAP systems."""
    # Process local material data
    local_material = process_local_material()
    local_material.printSchema()
    local_material.show(5)
    logger.info(f"Local Material data processed successfully. Count: {local_material.count()}")

    # Harmonize column names for local material
    local_material_rename_map = {
        "MATNR": "material_number",
        "WERKS": "plant",
        "SOURCE_SYSTEM_ERP": "source_system_erp",
        "MANDT": "client",
        "MEINS": "base_unit_of_measure",
        "GLOBAL_MATERIAL_NUMBER": "global_material_number",
        "BWKEY": "valuation_area",
        "NAME1": "branch_name",
        "VPRSV": "price_control_indicator",
        "VERPR": "moving_average_price",
        "STPRS": "standard_price",
        "PEINH": "price_unit",
        "BKLAS": "valuation_class",
        "BUKRS": "company_code",
        "WAERS": "currency_key",
        "primary_key_intra": "primary_key_intra",
        "primary_key_inter": "primary_key_inter",
        "global_mtl_id": "global_mtl_id",
    }

    for old_name, new_name in local_material_rename_map.items():
        local_material = local_material.withColumnRenamed(old_name, new_name)

    # Postprocess process order data
    process_order = process_process_order()
    process_order.printSchema()
    process_order.show(5)
    logger.info(f"Process Order data processed successfully. Count: {process_order.count()}")

    # Harmonize column names for process order
    process_order_rename_map = {
        "MATNR": "material_number",
        "AUFNR": "order_number",
        "SOURCE_SYSTEM_ERP": "source_system_erp",
        "MANDT": "client",
        "GLTRP": "planned_start_date",
        "GSTRP": "planned_finish_date",
        "FTRMS": "scheduled_start_date",
        "GLTRS": "actual_start_date",
        "GSTRS": "actual_finish_date",
        "GSTRI": "planned_start_date_internal",
        "GETRI": "planned_finish_date_internal",
        "GLTRI": "actual_start_date_internal",
        "FTRMI": "scheduled_start_date_internal",
        "FTRMP": "scheduled_finish_date_internal",
        "DISPO": "mrp_controller",
        "FEVOR": "production_supervisor",
        "PLGRP": "planner_group",
        "FHORI": "scheduling_direction",
        "AUFPL": "routing_number",
        "start_date": "start_date",
        "finish_date": "finish_date",
        "POSNR": "item_number",
        "DWERK": "plant",
        "MEINS": "base_unit_of_measure",
        "KDAUF": "sales_order",
        "KDPOS": "sales_order_item",
        "LTRMI": "requested_delivery_date",
        "OBJNR": "object_number",
        "ERDAT": "creation_date",
        "ERNAM": "created_by",
        "AUART": "order_type",
        "ZZGLTRP_ORIG": "original_planned_start_date",
        "ZZPRO_TEXT": "production_order_text",
        "GLOBAL_MATERIAL_NUMBER": "global_material_number",
        "NTGEW": "net_weight",
        "MTART": "material_type",
        "primary_key_intra": "primary_key_intra",
        "primary_key_inter": "primary_key_inter",
        "on_time_flag": "on_time_flag",
        "actual_on_time_deviation": "actual_on_time_deviation",
        "late_delivery_bucket": "late_delivery_bucket",
        "mto_vs_mts_flag": "mto_vs_mts_flag",
        "order_finish_timestamp": "order_finish_timestamp",
        "order_start_timestamp": "order_start_timestamp",
    }

    for old_name, new_name in process_order_rename_map.items():
        process_order = process_order.withColumnRenamed(old_name, new_name)

    local_material.printSchema()
    local_material.show(5)
    local_material.coalesce(1).write.csv(
        str(parent_dir_name / "data" / "output" / "local_material"), mode="overwrite", header=True
    )
    process_order.printSchema()
    process_order.show(5)
    process_order.coalesce(1).write.csv(
        str(parent_dir_name / "data" / "output" / "process_order"), mode="overwrite", header=True
    )


def main():
    """Main function to run the technical challenge."""
    harmonize_data()


if __name__ == "__main__":
    main()
