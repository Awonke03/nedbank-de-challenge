"""
pipeline/ingest.py  ─  Bronze Layer

Reads the three raw source files and writes Delta Parquet Bronze tables.
Called with an existing SparkSession — does NOT create or stop Spark.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.utils import get_logger, get_spark, load_config, write_delta

logger = get_logger("bronze.ingest")


def _read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header",      "true")
        .option("inferSchema", "false")
        .option("mode",        "PERMISSIVE")
        .option("encoding",    "UTF-8")
        .load(path)
    )


def _read_jsonl(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.format("json")
        .option("mode",          "PERMISSIVE")
        .option("encoding",      "UTF-8")
        .option("allowComments", "false")
        .load(path)
    )


def _add_audit(df: DataFrame, ingestion_ts: str) -> DataFrame:
    df = df.withColumn("_ingestion_timestamp", F.lit(ingestion_ts))
    if "_corrupt_record" in df.columns:
        df = df.withColumn(
            "_is_corrupt",
            F.col("_corrupt_record").isNotNull(),
        ).drop("_corrupt_record")
    else:
        df = df.withColumn("_is_corrupt", F.lit(False))
    return df


def run_ingestion(spark: SparkSession = None, cfg: dict = None) -> None:
    """
    Bronze ingestion.  Accepts an existing spark/cfg (shared-session mode)
    or bootstraps its own (standalone mode for backward-compat).
    """
    _own_spark = spark is None
    if cfg is None:
        cfg = load_config()
    if spark is None:
        spark = get_spark(cfg)

    bronze_base = cfg["output"]["bronze_path"]
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    try:
        accounts_path     = cfg["input"]["accounts_path"]
        customers_path    = cfg["input"]["customers_path"]
        transactions_path = cfg["input"]["transactions_path"]

        logger.info("Ingesting accounts from: %s", accounts_path)
        accounts = _add_audit(_read_csv(spark, accounts_path), ts)

        logger.info("Ingesting customers from: %s", customers_path)
        customers = _add_audit(_read_csv(spark, customers_path), ts)

        logger.info("Ingesting transactions from: %s", transactions_path)
        transactions = _add_audit(_read_jsonl(spark, transactions_path), ts)

        # Write all three — sequential to stay within 2GB memory
        logger.info("Writing Bronze layers...")
        write_delta(accounts,     f"{bronze_base}/accounts")
        write_delta(customers,    f"{bronze_base}/customers")
        write_delta(transactions, f"{bronze_base}/transactions")

        logger.info("Bronze ingestion complete.")
    except Exception as exc:
        logger.exception("Bronze ingestion failed: %s", exc)
        if _own_spark:
            spark.stop()
        sys.exit(1)
    finally:
        if _own_spark:
            spark.stop()


if __name__ == "__main__":
    run_ingestion()
