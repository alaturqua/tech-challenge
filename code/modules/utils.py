"""Utility functions for the Spark application."""

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812


def get_logger(name: str) -> logging.Logger:
    """Create a logger object with both console and file handlers.

    Args:
        name (str): Name of the logger.

    Returns:
        logging.Logger: Logger object.

    Raises:
        OSError: If unable to create logs directory or log file.
    """
    try:
        # Create logs directory if it doesn't exist
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        # Create logger
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        # Clear existing handlers to avoid duplicates
        if logger.handlers:
            logger.handlers.clear()

        # Create formatters
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s")

        # File handler
        file_handler = logging.FileHandler(filename=log_dir / "spark.log", encoding="utf-8", mode="a")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    except OSError as e:
        msg = f"Failed to setup logger: {e!s}"
        raise OSError(msg) from e
    else:
        return logger


logger = get_logger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session.

    Args:
        app_name (str): Name of the Spark application.

    Returns:
        SparkSession: Spark session object.
    """
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

    return SparkSession.builder.appName(app_name).config("spark.sql.repl.eagerEval.enablede", True).getOrCreate()


def read_csv_file(
    spark: SparkSession,
    file_directory: str,
    infer_schema: bool = True,
    schema: str | None = None,
) -> DataFrame:
    """Read a CSV file into a Spark DataFrame.

    Args:
        spark (SparkSession): Spark session object.
        file_directory (str): Path to the CSV file.
        infer_schema (bool): Whether to infer the schema of the CSV file.
        schema (str): Schema of the CSV file.

    Returns:
        DataFrame: Spark DataFrame object.
    """
    encoding = "utf-8"

    logger.info(f"Reading CSV file: {file_directory}")
    try:
        if infer_schema:
            csv_df = spark.read.csv(f"{file_directory}", header=True, inferSchema=True, encoding=encoding)
        else:
            csv_df = spark.read.csv(f"{file_directory}", header=True, schema=schema, encoding=encoding)

    except Exception as e:
        logger.exception("An error occurred while reading the CSV file.", extra=e)
        return None
    else:
        return csv_df


def profile_data(df: DataFrame) -> None:
    """Profile the data in a Spark DataFrame.

    Args:
        df (DataFrame): Spark DataFrame object.
    """
    try:
        logger.info("Data profiling results:")
        df.printSchema()

        df.describe().show()
        df.show(5)

    except Exception as e:
        logger.exception("An error occurred while profiling the data.", extra=e)


def mask_sensitive_columns(df: DataFrame, sensitive_columns: list) -> DataFrame:
    """Masks sensitive columns in the given Spark DataFrame.

    Args:
        df (DataFrame): Input Spark DataFrame.
        sensitive_columns (list): List of column names to mask.

    Returns:
        DataFrame: Spark DataFrame with masked sensitive columns.
    """
    logger.info(f"Masking sensitive columns: {sensitive_columns}")

    # Start with the original DataFrame
    df_masked = df

    # Iterate through the sensitive columns and apply sha256 hashing
    for col_name in sensitive_columns:
        if col_name in df.columns:
            # Apply hashing but only for non-null values
            df_masked = df_masked.withColumn(
                col_name,
                F.when(F.col(col_name).isNotNull(), F.sha2(F.col(col_name).cast("string"), 256)).otherwise(
                    F.col(col_name)
                ),
            )
        else:
            logger.error(f"Column '{col_name}' not found in DataFrame.")
            error_message = f"Column '{col_name}' not found in DataFrame."
            raise ValueError(error_message)

    return df_masked


def check_columns_unique(df: DataFrame, columns: list):
    """Checks if each column in the given list is unique in the DataFrame.

    Args:
        df (DataFrame): The input Spark DataFrame.
        columns (list): List of column names to check for uniqueness.

    Raises:
        ValueError: If any column contains duplicate values.
    """
    for column in columns:
        if column not in df.columns:
            error_message = f"Column '{column}' is not present in the DataFrame."
            raise ValueError(error_message)

        # Count duplicate values for the column
        duplicate_count = df.groupBy(column).count().filter(F.col("count") > 1).count()

        if duplicate_count > 0:
            error_message = f"Column '{column}' is not unique. Found {duplicate_count} duplicate values."
            raise ValueError(error_message)

    logger.info(f"All specified columns {columns} are unique.")
