"""Module to prepare local material data for harmonization across systems."""

from modules.utils import get_logger
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F  # noqa: N812

logger = get_logger(__name__)


def prep_general_material_data(
    df: DataFrame,
    col_mara_global_material_number: str,
    check_old_material_number_is_valid: bool = True,
    check_material_is_not_deleted: bool = True,
) -> DataFrame:
    """Prepare General Material data for harmonization across systems.

    Input Data: SAP MARA table (General Material Data)

    Transformation:
        ○ Filter materials:
            ▪ Old Material Number (BISMT) is not in ["ARCHIVE", "DUPLICATE", "RENUMBERED"] or is null.
            ▪ Deletion flag (LVORM) is null or empty.
        ○ Select the required columns.
        ○ Rename the global material number column to a consistent name, if necessary.

    Args:
        df (DataFrame): DataFrame with General Material data.
        col_mara_global_material_number (str): Column name with the global material number.
        check_old_material_number_is_valid (bool): Check if the old material number is valid.
        check_material_is_not_deleted (bool): Check if the material is not deleted.

    Returns:
        DataFrame: DataFrame with prepared General Material data.

    Raises:
        TypeError: If the input DataFrame is not a DataFrame.
        TypeError: If the column name with the global material number is not a string.
        TypeError: If the check for the old material number is not a boolean.
        TypeError: If the check for the material deletion flag is not a boolean.
    """
    logger.info("Preparing General Material data")
    if None or not isinstance(df, DataFrame):
        error_message = "df must be a DataFrame"
        raise TypeError(error_message)
    if None or not isinstance(col_mara_global_material_number, str):
        error_message = "col_mara_global_material_number must be a string"
        raise TypeError(error_message)
    if None or not isinstance(check_old_material_number_is_valid, bool):
        error_message = "check_old_material_number_is_valid must be a boolean"
        raise TypeError(error_message)
    if None or not isinstance(check_material_is_not_deleted, bool):
        error_message = "check_material_is_not_deleted must be a boolean"
        raise TypeError(error_message)

    # Start with the original DataFrame
    sap_mara = df

    # Apply old material number check if enabled
    if check_old_material_number_is_valid:
        sap_mara = sap_mara.filter(
            F.col("BISMT").isNull() | ~F.col("BISMT").isin(["ARCHIVE", "DUPLICATE", "RENUMBERED"])
        )

    # Apply deletion flag check if enabled
    if check_material_is_not_deleted:
        sap_mara = sap_mara.filter(F.col("LVORM").isNull() | (F.trim(F.col("LVORM")).cast("string") == F.lit("")))

    # Select and rename columns
    columns = [
        "MANDT",  # Client
        "MATNR",  # Material Number
        "MEINS",  # Base Unit of Measure
        F.col(col_mara_global_material_number).alias("GLOBAL_MATERIAL_NUMBER"),  # Global Material Number
    ]

    return sap_mara.select(*columns)


def prep_material_valuation(df: DataFrame) -> DataFrame:
    """Prepare Material Valuation data for harmonization across systems.

    Input Data: SAP MBEW table (Material Valuation)

    Transformation:
      ○ Filter out materials that are flagged for deletion (LVORM is null).
      ○ Filter for entries with BWTAR (Valuation Type) as null to exclude split valuation materials.
      ○ Deduplicate records:
        ▪ Rule take the record having highest evaluated price LAEPR (Last Evaluated Price) at MATNR and BWKEY level
    Keep the first record per group.
        ○ Select the required columns.
        ○ Drop Duplicates.

    Args:
        df (DataFrame): DataFrame with Material Valuation data.

    Returns:
        DataFrame: DataFrame with prepared Material Valuation data.

    Raises:
        TypeError: If the input DataFrame is not a DataFrame.
    """
    logger.info("Preparing Material Valuation data")
    if None or not isinstance(df, DataFrame):
        error_message = "df must be a DataFrame"
        raise TypeError(error_message)

    window_spec = Window.partitionBy("MATNR", "BWKEY").orderBy(F.col("LAEPR").desc())

    # Start with the original DataFrame
    sap_mbew = (
        df.filter(F.col("LVORM").isNull())  # Filter out materials that are flagged for deletion (LVORM is null).
        .filter(
            F.col("BWTAR").isNull()
        )  # Filter for entries with BWTAR (Valuation Type) as null to exclude split valuation materials.
        .withColumn(
            "ROW_NUMBER", F.row_number().over(window_spec)
        )  # Add row number based on the descending order of LAEPR.
        .filter(F.col("ROW_NUMBER") == 1)  # Filter for the latest price based on the row number.
        .drop("ROW_NUMBER")  # Drop the row number column.
    )

    # Select and rename columns
    columns = [
        "MANDT",  # Client
        "MATNR",  # Material Number
        "BWKEY",  # Valuation Area
        "VPRSV",  # Price Control Indicator
        "VERPR",  # Moving Average Price
        "STPRS",  # Standard Price
        "PEINH",  # Price Unit
        "BKLAS",  # Valuation Class
    ]

    return sap_mbew.select(*columns).drop_duplicates()


def prep_plant_data_for_material(
    df: DataFrame,
    check_deletion_flag_is_null: bool,
    drop_duplicate_records: bool,
    additional_fields: list | None = None,
) -> DataFrame:
    """Prepare Plant data for harmonization across systems.

    Input Data: SAP MARC table (Plant Data for Material)

    Transformation:
      ○ Filter records where the deletion flag (LVORM) is null.
      ○ Select the required columns.
      ○ Drop Duplicates if drop_duplicate_records is True.

    Args:
        df (DataFrame): DataFrame with Plant data for Material.
        check_deletion_flag_is_null (bool): Check if the deletion flag is null.
        drop_duplicate_records (bool): Drop duplicate records
        additional_fields (list): Additional fields to include in the output DataFrame.

    Returns:
        DataFrame: DataFrame with prepared Plant data.

    Raises:
        TypeError: If the input DataFrame is not a DataFrame.
        TypeError: If the check for the deletion flag is not a boolean.
        TypeError: If the drop duplicate records flag is not a boolean.
    """
    logger.info("Preparing Plant data for Material")
    if not isinstance(df, DataFrame):
        error_message = "df must be a DataFrame"
        raise TypeError(error_message)
    if not isinstance(check_deletion_flag_is_null, bool):
        error_message = "check_deletion_flag_is_null must be a boolean"
        raise TypeError(error_message)
    if not isinstance(drop_duplicate_records, bool):
        error_message = "drop_duplicate_records must be a boolean"
        raise TypeError(error_message)
    if additional_fields and not isinstance(additional_fields, list):
        error_message = "additional_fields must be a list of columns"
        raise TypeError(error_message)

    sap_marc = df

    # Apply deletion flag filter before selecting columns
    if check_deletion_flag_is_null:
        sap_marc = sap_marc.filter(F.col("LVORM").isNull())

    # Select columns
    columns = [
        "SOURCE_SYSTEM_ERP",  # Source ERP system identifier
        "MATNR",  # Material Number
        "WERKS",  # Plant
    ]

    if additional_fields:
        columns.extend([F.col(field) for field in additional_fields])

    sap_marc = sap_marc.select(*columns)

    if drop_duplicate_records:
        sap_marc = sap_marc.drop_duplicates()

    return sap_marc


def prep_plant_and_branches(df: DataFrame) -> DataFrame:
    """Prepare Plant and Branches data for harmonization across systems.

    Input Data: SAP T001W table (Plant/Branch)

    Transformation:
      ○ Select the required columns.

    Args:
        df (DataFrame): DataFrame with Plant and Branches data.

    Returns:
        DataFrame:  DataFrame with prepared Plant and Branches data.

    Raises:
        TypeError: If the input DataFrame is not a DataFrame.
    """
    logger.info("Preparing Plant and Branches data")
    if not isinstance(df, DataFrame):
        error_message = "df must be a DataFrame"
        raise TypeError(error_message)

    columns = [
        "MANDT",  # Client
        "WERKS",  # Plant
        "BWKEY",  # Valuation Area
        "NAME1",  # Name of Plant/Branch
    ]

    return df.select(*columns)


def prep_valuation_data(df: DataFrame) -> DataFrame:
    """Prepare Valuation data for harmonization across systems.

    Input Data: SAP T001K table (Valuation Area)

    Transformation:
      ○ Select the required columns.
      ○ Drop Duplicates to ensure uniqueness.

    Args:
        df (DataFrame): DataFrame with Valuation data.

    Returns:
        DataFrame: DataFrame with prepared Valuation data.

    Raises:
        TypeError: If the input DataFrame is not a DataFrame.
    """
    logger.info("Preparing Valuation data")
    if not isinstance(df, DataFrame):
        error_message = "df must be a DataFrame"
        raise TypeError(error_message)

    columns = [
        "MANDT",  # Client
        "BWKEY",  # Valuation Area
        "BUKRS",  # Company Code
    ]

    return df.select(*columns).drop_duplicates()


def prep_company_codes(df: DataFrame) -> DataFrame:
    """Prepare Company Codes data for harmonization across systems.

    Input Data: SAP T001 table (Company Codes)

    Transformation:
      ○ Select the required columns.

    Args:
        df (DataFrame): DataFrame with Company Codes data.

    Returns:
        DataFrame: DataFrame with prepared Company Codes data.

    Raises:
        TypeError: If the input DataFrame is not a DataFrame.
    """
    logger.info("Preparing Company Codes data")
    if not isinstance(df, DataFrame):
        error_message = "df must be a DataFrame"
        raise TypeError(error_message)

    columns = [
        "MANDT",  # Client
        "BUKRS",  # Company Code
        "WAERS",  # Currency Key
    ]

    return df.select(*columns)


def integration(
    sap_mara: DataFrame,
    sap_mbew: DataFrame,
    sap_marc: DataFrame,
    sap_t001k: DataFrame,
    sap_t001w: DataFrame,
    sap_t001: DataFrame,
) -> DataFrame:
    """Integrate data from various SAP tables to create a comprehensive dataset.

    Input Data: DataFrames from the following SAP tables:
        ○ SAP MARA table (General Material Data)
        ○ SAP MBEW table (Material Valuation)
        ○ SAP MARC table (Plant Data for Material)
        ○ SAP T001K table (Valuation Area)
        ○ SAP T001W table (Plant/Branch)
        ○ SAP T001 table (Company Codes)

    Transformation:
      ○ Join operations:
          ▪ sap_marc left join sap_mara on MATNR.
          ▪ left join sap_t001w on MANDT and WERKS.
          ▪ left join sap_mbew on MANDT, MATNR, and BWKEY.
          ▪ left join sap_t001k on MANDT and BWKEY.
          ▪ left join sap_t001 on MANDT and BUKRS.

    Args:
        sap_mara (DataFrame): General Material Data.
        sap_mbew (DataFrame): Material Valuation Data.
        sap_marc (DataFrame): Plant Data for Material.
        sap_t001k (DataFrame): Valuation Area Data.
        sap_t001w (DataFrame): Plant and Branches Data.
        sap_t001 (DataFrame): Company Codes Data.

    Returns:
        DataFrame: Integrated DataFrame with all necessary information.
    """
    logger.info("Joining SAP tables")
    # Join the DataFrames to create a comprehensive dataset
    return (
        sap_marc.join(sap_mara, on="MATNR", how="left")  # Join with sap_mara
        .join(sap_t001w, on=["MANDT", "WERKS"], how="left")  # Join with sap_t001w
        .join(sap_mbew, on=["MANDT", "MATNR", "BWKEY"], how="left")  # Join with sap_mbew
        .join(sap_t001k, on=["MANDT", "BWKEY"], how="left")  # Join with sap_t001k
        .join(sap_t001, on=["MANDT", "BUKRS"], how="left")  # Join with sap_t001
        .select(
            "MANDT",
            "SOURCE_SYSTEM_ERP",
            "MATNR",
            "WERKS",
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
        )
    )


def derive_intra_and_inter_primary_key(df: DataFrame) -> DataFrame:
    """Derive primary keys for harmonized data.

    Args:
        df (DataFrame): DataFrame with harmonized data.

    Returns:
        DataFrame: DataFrame with derived primary keys.
    """
    logger.info("Deriving primary keys")
    # Replace nulls with empty strings before concatenation
    return df.withColumn(
        "primary_key_intra",
        F.concat_ws("-", F.coalesce(F.col("MATNR"), F.lit("")), F.coalesce(F.col("WERKS"), F.lit(""))),
    ).withColumn(
        "primary_key_inter",
        F.concat_ws(
            "-",
            F.coalesce(F.col("SOURCE_SYSTEM_ERP"), F.lit("")),
            F.coalesce(F.col("MATNR"), F.lit("")),
            F.coalesce(F.col("WERKS"), F.lit("")),
        ),
    )


def post_prep_local_material(df: DataFrame) -> DataFrame:
    """Post-process the integrated DataFrame to create final local material data.

    Input Data: Resulting DataFrame from the integration step.

    Transformation:
      ○ Create mtl_plant_emd: Concatenate WERKS and NAME1 with a hyphen.
      ○ Assign global_mtl_id from MATNR or the global material number, as appropriate.
      ○ Derive two Derive the following keys (intra represents the primary key of one system and inter the primary key
      the harmonized view.

          ▪ Primary Key (primary_key_intra): Concatenate MATNR and WERKS.
          ▪ Primary Key (primary_key_inter): Concatenate SOURCE_SYSTEM_ERP, MATNR, and WERKS.
      ○ Handle Duplicates:
          ▪ Add a temporary column no_of_duplicates indicating duplicate counts.
          ▪ Drop Duplicates based on SOURCE_SYSTEM_ERP, MATNR, and WERKS.

    Args:
        df (DataFrame): Integrated DataFrame from the integration step.

    Returns:
        DataFrame: Final DataFrame with post-processed local material data.
    """
    logger.info("Post-processing Local Material data")
    # Add timestamp column to the DataFrame
    df_with_timestamp = df.withColumn("timestamp", F.current_timestamp())

    # Window spec to get the latest record based on timestamp
    window_spec = Window.partitionBy("SOURCE_SYSTEM_ERP", "MATNR", "WERKS").orderBy(F.col("timestamp").desc())

    df_with_primary_keys = derive_intra_and_inter_primary_key(df_with_timestamp)

    return (
        df_with_primary_keys.withColumn("mtl_plant_emd", F.concat_ws("-", F.col("WERKS"), F.col("NAME1")))
        .withColumn("global_mtl_id", F.coalesce(F.col("GLOBAL_MATERIAL_NUMBER"), F.col("MATNR")))
        .withColumn("row_number", F.row_number().over(window_spec))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .drop("timestamp")
    )
