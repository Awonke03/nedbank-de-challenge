"""
pipeline/utils.py
─────────────────
Shared helpers: Spark session, config loader, Delta read/write,
surrogate key generation.  No business logic here.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType


# ── Environment fixes applied before any Spark code runs ─────────────────────

# Fix 1 — SPARK_HOME
#   The base image sets SPARK_HOME to dist-packages/pyspark, but pip places
#   PySpark under site-packages.  Derive the real path from the imported
#   module so spark-submit is always found regardless of base image ENV.
import pyspark as _pyspark
os.environ["SPARK_HOME"] = str(Path(_pyspark.__file__).parent)

# Fix 2 — SPARK_LOCAL_IP / driver bind address
#   The container runs with --network=none so hostname DNS resolution fails.
#   Pinning to 127.0.0.1 bypasses all hostname lookups for the Spark driver.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


# ── Logging ──────────────────────────────────────────────────────────────────

def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )
    return logging.getLogger(name)


# ── Config loader ─────────────────────────────────────────────────────────────

def load_config() -> dict[str, Any]:
    """
    Load pipeline_config.yaml.
    Search order:
      1. PIPELINE_CONFIG env var
      2. /data/config/pipeline_config.yaml  (scoring system mount)
      3. repo-local config/pipeline_config.yaml
    """
    candidates = [
        os.environ.get("PIPELINE_CONFIG"),
        "/data/config/pipeline_config.yaml",
        str(Path(__file__).parent.parent / "config" / "pipeline_config.yaml"),
    ]
    for path in candidates:
        if path and Path(path).exists():
            with open(path) as fh:
                cfg = yaml.safe_load(fh)
            return cfg
    raise FileNotFoundError(
        "pipeline_config.yaml not found. Checked: " + str(candidates)
    )


# ── Spark session ─────────────────────────────────────────────────────────────

def get_spark(cfg: dict[str, Any]) -> SparkSession:
    """
    Build and return a SparkSession with Delta Lake enabled.

    The Delta JARs (delta-spark_2.12-3.1.0.jar and delta-storage-3.1.0.jar)
    are baked into the image under $SPARK_HOME/jars/ at build time, so they
    are on PySpark's default classpath without --packages or Ivy resolution.
    """
    sc = cfg.get("spark", {})

    spark = (
        SparkSession.builder
        .appName(sc.get("app_name", "nedbank-de-pipeline"))
        .master(sc.get("master", "local[2]"))
        # Bind driver to loopback so no DNS resolution is attempted for the
        # container hostname, which has no resolver with --network=none.
        .config("spark.driver.host",                                  "127.0.0.1")
        .config("spark.driver.bindAddress",                           "127.0.0.1")
        # Resource limits
        .config("spark.driver.memory",                                sc.get("driver_memory", "1500m"))
        .config("spark.driver.maxResultSize",                         "512m")
        .config("spark.sql.shuffle.partitions",                       str(sc.get("shuffle_partitions", 8)))
        .config("spark.sql.adaptive.enabled",                         str(sc.get("adaptive_enabled", True)).lower())
        .config("spark.sql.files.maxPartitionBytes",                  "67108864")
        # Delta Lake extensions — JARs already on classpath from image build
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        # Use zstd instead of Snappy for Parquet compression.
        # Snappy extracts a native .so to /tmp then mmap-executes it, which
        # fails under Docker Desktop's tmpfs (noexec semantics on mapped pages).
        # zstd is pure-Java — no native library, no /tmp writes.
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.hadoop.parquet.compression.codec", "uncompressed")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Delta helpers ─────────────────────────────────────────────────────────────

def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


# ── Surrogate key (BIGINT, deterministic) ─────────────────────────────────────

def add_surrogate_key(df: DataFrame, key_col: str, natural_key_col: str) -> DataFrame:
    """
    Add a BIGINT surrogate key derived from the SHA-256 hash of the natural key.
    Takes the first 15 hex characters of SHA-256 and converts base-16 → base-10.
    15 hex chars = max ~1.15e18, safely within BIGINT range (~9.2e18).
    Deterministic: same natural key always produces the same surrogate key.
    """
    return df.withColumn(
        key_col,
        F.conv(
            F.substring(
                F.sha2(F.coalesce(F.col(natural_key_col).cast("string"), F.lit("")), 256),
                1, 15,
            ),
            16,  # from base
            10,  # to base
        ).cast(LongType()),
    )