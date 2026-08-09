"""SparkSession construction with Delta Lake wired in.

On Databricks the session already exists and already has Delta — we attach to it and
only apply our own settings. Locally we build one from scratch, pulling the Delta
jars via ``delta-spark``'s configure_spark_with_delta_pip helper.

The version pins matter: delta-spark 3.2.0 pairs with Spark 3.5.x, which is what
Databricks Runtime 15.4 LTS ships. Mixing versions produces protocol errors that
only appear when writing, not when starting up.
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import TYPE_CHECKING

from medchain.config import Config, load_config

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import SparkSession


def _ensure_java_home() -> None:
    """Point JAVA_HOME at a Spark-compatible JDK if the ambient one is too new.

    Fedora ships JDK 25/26 as the system Java; Spark 3.5 supports 8/11/17 only and
    fails with an opaque ``UnsupportedClassVersionError`` / illegal-reflective-access
    crash otherwise. We prefer an explicitly exported JAVA_HOME, then the
    project-local Temurin 17 that ``make setup`` installs.
    """
    current = os.environ.get("JAVA_HOME")
    if current and Path(current, "bin", "java").exists():
        return

    for candidate in (
        Path.home() / ".local" / "jdks" / "jdk-17",
        Path("/usr/lib/jvm/java-17-openjdk"),
        Path("/usr/lib/jvm/temurin-17-jdk"),
    ):
        if (candidate / "bin" / "java").exists():
            os.environ["JAVA_HOME"] = str(candidate)
            return

    # Not fatal — the user may be on Databricks or have a compatible default JDK.


def is_databricks() -> bool:
    """True when running inside a Databricks cluster."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(cfg: Config | None = None) -> SparkSession:
    """Return a configured SparkSession, reusing the active one where possible."""
    cfg = cfg or load_config()
    _ensure_java_home()

    from pyspark.sql import SparkSession

    if is_databricks():
        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        for key, value in cfg.spark_conf.items():
            # Some cluster-level settings are immutable at runtime; skipping them is
            # correct, because the cluster policy already set them.
            with contextlib.suppress(Exception):
                spark.conf.set(key, value)
        return spark

    from delta import configure_spark_with_delta_pip

    builder = SparkSession.builder.appName(cfg.app_name)
    if cfg.spark_master:
        builder = builder.master(cfg.spark_master)

    builder = (
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Spark 3.5 on JDK 17 needs these opens for its Arrow/Unsafe usage.
        .config(
            "spark.driver.extraJavaOptions",
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        )
    )

    for key, value in cfg.spark_conf.items():
        builder = builder.config(key, value)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark() -> None:
    """Stop the active session, if any. Used by test teardown."""
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is not None:
        session.stop()
