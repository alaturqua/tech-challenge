"""Module to prepare SAP order data."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import DateType, DoubleType


def prep_sap_order_header_data(df: DataFrame) -> DataFrame:
    """Prepare the SAP order header data.

    Input Data: SAP AFKO table (Order Header Data)

    Transformation:
        ○ Select the required columns.
        ○ Createstart_date and finish_date:
            ▪ Format GSTRP as 'yyyy-MM'.
            ▪ If GSTRP is null, use the current date.
            ▪ Concatenate'-01' to form full dates.
            ▪ Convert to date format.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: Processed DataFrame

    Raises:
        TypeError: If input DataFrame is invalid
    """
    if df is None or isinstance(df, DataFrame) is False:
        error_message = "Invalid input DataFrame"
        raise TypeError(error_message)

    columns = [
        "SOURCE_SYSTEM_ERP",  # Source ERP system identifier
        "MANDT",  # Client
        "AUFNR",  # Order number
        F.col("GLTRP").cast(DateType()),  # Basic finish date
        F.col("GSTRP").cast(DateType()),  # Basic start date
        F.col("FTRMS").cast(DateType()),  # Scheduled finish date
        F.col("GLTRS").cast(DateType()),  # Actual finish date
        F.col("GSTRS").cast(DateType()),  # Actual start date
        F.col("GSTRI").cast(DateType()),  # Basic finish date
        F.col("GETRI").cast(DateType()),  # Basic start date
        F.col("GLTRI").cast(DateType()),  # Scheduled finish date
        F.col("FTRMI").cast(DateType()),  # Actual finish date
        F.col("FTRMP").cast(DateType()),  # Actual start date
        "DISPO",  # MRP controller
        "FEVOR",  # Production Supervisor
        "PLGRP",  # Planner Group
        "FHORI",  # Scheduling Margin Key for Floats
        "AUFPL",  # Routing number of operations in the order
    ]

    return (
        df.select(*columns)
        .withColumn(
            "start_date",
            F.when(F.col("GSTRP").isNull(), F.current_date()).otherwise(F.col("GSTRP")),
        )
        .withColumn(
            "finish_date",
            F.when(F.col("GLTRP").isNull(), F.current_date()).otherwise(F.col("GLTRP")),
        )
        .withColumn(
            "start_date",
            F.date_format(
                F.concat_ws("-", F.year(F.col("start_date")), F.month(F.col("start_date")), F.lit("01")), "yyyy-MM-dd"
            ).cast(DateType()),
        )
        .withColumn(
            "finish_date",
            F.date_format(
                F.concat_ws("-", F.year(F.col("finish_date")), F.month(F.col("finish_date")), F.lit("01")), "yyyy-MM-dd"
            ).cast(DateType()),
        )
    )


def prep_sap_order_item(df: DataFrame) -> DataFrame:
    """Prepare the SAP order item data.

    Input Data: SAP AFPO table (Order Item Data)
    Transformation:
        ○ Select the required columns.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: Processed DataFrame

    Raises:
        TypeError: If input DataFrame is invalid
    """
    if df is None or isinstance(df, DataFrame) is False:
        error_message = "Invalid input DataFrame"
        raise TypeError(error_message)

    columns = [
        "AUFNR",  # Order number
        "POSNR",  # Order Item number
        "DWERK",  # Plant
        "MATNR",  # Material number
        "MEINS",  # Base unit of measure
        "KDAUF",  # Sales order number
        "KDPOS",  # Sales order item
        F.col("LTRMI").cast(DateType()),  # Actual delivery/finish date
    ]

    return df.select(*columns)


def prep_sap_order_master_data(df: DataFrame) -> DataFrame:
    """Prepare the SAP order master data.

    Input Data: SAP AUFK table (Order Master Data)

    Transformation:
        ○ Select the required columns.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: Processed DataFrame

    Raises:
        TypeError: If input DataFrame is invalid
    """
    if df is None or isinstance(df, DataFrame) is False:
        error_message = "Invalid input DataFrame"
        raise TypeError(error_message)

    # Selected columns
    columns = [
        "AUFNR",  # Order Number
        "OBJNR",  # Object Number
        F.col("ERDAT").cast(DateType()),  # Creation Date
        "ERNAM",  # Created By
        "AUART",  # Order type
        F.col("ZZGLTRP_ORIG").cast(DateType()),  # Original Basic Finish Date,
        "ZZPRO_TEXT",  # Project Text
    ]

    return df.select(*columns)


def prep_sap_general_material_data(df: DataFrame, col_global_material: str) -> DataFrame:
    """Prepare the SAP general material data.

    Input Data: SAP MARA table (General Material Data)

    Transformation:
        ○ Filter materials:
            ▪ Old Material Number (BISMT) is not in ["ARCHIVE", "DUPLICATE", "RENUMBERED"] or is null.
            ▪ Deletion flag (LVORM) is null or empty.
        ○ Select the required columns.
        ○ Rename the global material number column to a consistent name, if necessary.

    Args:
        df (DataFrame): Input DataFrame
        col_global_material (str): Column name for Global Material

    Returns:
        DataFrame: Processed DataFrame

    Raises:
        TypeError: If input DataFrame is invalid
        TypeError: If column name is invalid
    """
    if df is None or isinstance(df, DataFrame) is False:
        error_message = "Invalid input DataFrame"
        raise TypeError(error_message)
    if col_global_material is None or not isinstance(col_global_material, str):
        error_message = "Invalid input column name"
        raise TypeError(error_message)

    df_filtered = df.filter(
        (F.col("BISMT").isNull() | ~F.col("BISMT").isin(["ARCHIVE", "DUPLICATE", "RENUMBERED"]))
        & (F.col("LVORM").isNull() | (F.trim(F.col("LVORM")).cast("string") == F.lit("")))
    )

    columns = [
        "MATNR",  # Material Number
        F.col(col_global_material).alias("GLOBAL_MATERIAL_NUMBER"),  # Global Material Number
        "NTGEW",  # Net Weight
        "MTART",  # Material Type
    ]

    return df_filtered.select(*columns)


def integration(
    sap_afko: DataFrame,
    sap_afpo: DataFrame,
    sap_aufk: DataFrame,
    sap_mara: DataFrame,
    sap_cdpos: DataFrame | None = None,
) -> DataFrame:
    """Integration of SAP tables.

    ● DataFrames to Integrate:
        ○ Order Header Data (sap_afko)
        ○ Order Item Data (sap_afpo)
        ○ Order Master Data (sap_aufk)
        ○ General Material Data (sap_mara)
        ○ Header and Item (sap_cdpos, optional)

    Transformation:
        ○ Join operations:
            ▪ sap_afko left join sap_afpo on AUFNR.
            ▪ Result left join sap_aufk on AUFNR.
            ▪ Result left join sap_mara on MATNR.
            ▪ If sap_cdpos is provided, result left join sap_cdpos on OBJNR.
        ○ Handle Missing Values:
            ▪ Use ZZGLTRP_ORIG if available; otherwise, use GLTRP.

    Args:
        sap_afko (DataFrame): SAP AFKO Dataframe
        sap_afpo (DataFrame): SAP AFPO Dataframe
        sap_aufk (DataFrame): SAP AUFK Dataframe
        sap_mara (DataFrame): SAP MARA Dataframe
        sap_cdpos (DataFrame, optional): SAP CDPOS Dataframe. Defaults to None.

    Returns:
        DataFrame: Integrated DataFrame
    """
    # Validate input types
    for arg, name in zip(
        [sap_afko, sap_afpo, sap_aufk, sap_mara], ["sap_afko", "sap_afpo", "sap_aufk", "sap_mara"], strict=True
    ):
        if not isinstance(arg, DataFrame):
            error_message = f"Expected DataFrame for {name}, got {type(arg).__name__} instead."
            raise TypeError(error_message)

    # Optional parameter check
    if sap_cdpos is not None and not isinstance(sap_cdpos, DataFrame):
        error_message = f"Expected DataFrame or None for sap_cdpos, got {type(sap_cdpos).__name__} instead."
        raise TypeError(error_message)

    result = (
        sap_afko.join(sap_afpo, on="AUFNR", how="left")
        .join(sap_aufk, on="AUFNR", how="left")
        .join(sap_mara, on="MATNR", how="left")
    )
    if sap_cdpos:
        result = result.join(sap_cdpos, on="OBJNR", how="left")

    return result.withColumn("ZZGLTRP_ORIG", F.coalesce(F.col("ZZGLTRP_ORIG"), F.col("GLTRP")))


def post_prep_process_order(df: DataFrame) -> DataFrame:
    """Post-process the Process Order Data.

    Input Data: Integrated DataFrame from the joined SAP tables.

    Transformation:
        ○ Derive the following keys (intra represents the primary key of
          one system and inter the primary key the harmonized
        view.
            ▪ Intra Primary Key (primary_key_intra): Concatenate AUFNR, POSNR, and DWERK.
            ▪ Inter Primary Key (primary_key_inter): Concatenate SOURCE_SYSTEM_ERP, AUFNR, POSNR, and DWERK.
        ○ Calculate On-Time Flag:
            ▪ Set on_time_flag to:
                ● 1 if ZZGLTRP_ORIG >= LTRMI.
                ● 0 if ZZGLTRP_ORIG < LTRMI.
                ● null if dates are missing.
        ○ Calculate On-Time Deviation and Late Delivery Bucket:
            ▪ Compute actual_on_time_deviation as ZZGLTRP_ORIG - LTRMI.
            ▪ Categorize late_delivery_bucket based on deviation days.
        ○ Ensure ZZGLTRP_ORIG is Present:
            ▪ Add ZZGLTRP_ORIG with null values if it's not in the DataFrame.
        ○ Derive MTO vs. MTS Flag:
            ▪ Set mto_vs_mts_flag to:
                ● "MTO" if KDAUF (Sales Order Number) is not null.
                ● "MTS" otherwise.
        ○ Convert Dates to Timestamps:
            ▪ Create order_finish_timestamp from LTRMI.
            ▪ Create order_start_timestamp from GSTRI.

    Args:
        df (DataFrame): Resulting DataFrame from the integration step.

    Returns:
        DataFrame: Post-processed DataFrame with additional calculated fields.
    """
    # Ensure ZZGLTRP_ORIG is present
    if "ZZGLTRP_ORIG" not in df.columns:
        post_processed_data = df.withColumn("ZZGLTRP_ORIG", F.lit(None).cast("date"))

    # Cast date fields to DateType
    post_processed_data = (
        df.withColumn("ZZGLTRP_ORIG", F.col("ZZGLTRP_ORIG").cast("date"))
        .withColumn("LTRMI", F.col("LTRMI").cast("date"))
        .withColumn("GSTRI", F.col("GSTRI").cast("date"))
    )

    return (
        post_processed_data
        # Derive Primary Keys
        .withColumn("primary_key_intra", F.concat_ws("_", F.col("AUFNR"), F.col("POSNR"), F.col("DWERK")))
        .withColumn(
            "primary_key_inter",
            F.concat_ws("_", F.col("SOURCE_SYSTEM_ERP"), F.col("AUFNR"), F.col("POSNR"), F.col("DWERK")),
        )
        # Calculate On-Time Flag
        .withColumn(
            "on_time_flag",
            F.when(
                F.col("ZZGLTRP_ORIG").isNotNull()
                & F.col("LTRMI").isNotNull()
                & (F.col("ZZGLTRP_ORIG") >= F.col("LTRMI")),
                F.lit(1),
            )
            .when(
                F.col("ZZGLTRP_ORIG").isNotNull()
                & F.col("LTRMI").isNotNull()
                & (F.col("ZZGLTRP_ORIG") < F.col("LTRMI")),
                F.lit(0),
            )
            .otherwise(F.lit(None)),
        )
        # Calculate On-Time Deviation
        .withColumn(
            "actual_on_time_deviation",
            F.when(
                F.col("ZZGLTRP_ORIG").isNotNull() & F.col("LTRMI").isNotNull(),
                F.datediff(F.col("ZZGLTRP_ORIG"), F.col("LTRMI")),
            )
            .otherwise(F.lit(None))
            .cast(DoubleType()),
        )
        # Categorize Late Delivery Bucket
        .withColumn(
            "late_delivery_bucket",
            F.when(F.col("actual_on_time_deviation") <= 0, F.lit("On Time"))
            .when(F.col("actual_on_time_deviation").between(1, 7), F.lit("1-7 Days Late"))
            .when(F.col("actual_on_time_deviation").between(8, 14), F.lit("8-14 Days Late"))
            .when(F.col("actual_on_time_deviation") > 14, F.lit(">14 Days Late"))
            .otherwise(F.lit("Unknown")),
        )
        # Derive MTO vs. MTS Flag
        .withColumn(
            "mto_vs_mts_flag",
            F.when(F.col("KDAUF").isNotNull(), F.lit("MTO")).otherwise(F.lit("MTS")),
        )
        # Convert Dates to Timestamps
        .withColumn("order_finish_timestamp", F.to_timestamp(F.col("LTRMI")))
        .withColumn("order_start_timestamp", F.to_timestamp(F.col("GSTRI")))
    )
