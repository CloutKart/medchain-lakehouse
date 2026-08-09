"""Shared pytest fixtures.

The Spark session is session-scoped and deliberately small: 2 shuffle partitions and
a local[2] master. Test suites that leave Spark at its 200-partition default spend
most of their runtime scheduling empty tasks.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pytest

os.environ.setdefault("MEDCHAIN_ENV", "local")


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession with Delta, shared across the whole test session."""
    from medchain.spark import _ensure_java_home

    _ensure_java_home()

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("medchain-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config(
            "spark.driver.extraJavaOptions",
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def tmp_delta_dir():
    """A throwaway directory for Delta tables, removed after the test."""
    path = Path(tempfile.mkdtemp(prefix="medchain-test-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def cfg():
    from medchain.config import load_config

    return load_config("local")
