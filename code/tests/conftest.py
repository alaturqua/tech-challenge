"""This module contains the fixtures for the tests."""

from typing import Any, Generator

import findspark
import pytest
from pyspark.sql import SparkSession


# Create fixture for SparkSession
@pytest.fixture(scope="session")
def spark() -> Generator[Any, Any, Any]:
    """Fixture for creating a Spark session.

    Yields:
        SparkSession: Spark session object.
    """
    findspark.init()
    spark = SparkSession.builder.appName("pytest").getOrCreate()

    yield spark
    spark.stop()
