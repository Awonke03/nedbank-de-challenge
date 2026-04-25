"""
pipeline/transform.py  ─  Silver Layer  (Bronze → Silver)

Called with an existing SparkSession — does NOT create or stop Spark.
"""
from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.window import Window

from pipeline.utils import get_logger, get_spark, load_config, read_delta, write_delta

logger = get_logger("silver.transform")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_date(col_name: str):
    return F.coalesce(
        F.to_date(F.col(col_name), "yyyy-MM-dd"),
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "dd-MM-yyyy"),
        F.to_date(F.col(col_name), "MM/dd/yyyy"),
        F.to_date(F.col(col_name), "yyyyMMdd"),
        F.to_date(F.from_unixtime(F.col(col_name).cast("long")), "yyyy-MM-dd"),
    )


def _dedup(df: DataFrame, pk: str) -> DataFrame:
    w = Window.partitionBy(pk).orderBy(F.col("_ingestion_timestamp").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def _drop_corrupt(df: DataFrame) -> DataFrame:
    return df.filter(~F.col("_is_corrupt"))


# ── accounts ──────────────────────────────────────────────────────────────────

def _transform_accounts(bronze: DataFrame) -> DataFrame:
    logger.info("Transforming accounts...")
    df = _drop_corrupt(bronze)
    df = df.withColumn(
        "_dq_null_pk",
        F.col("account_id").isNull() | (F.trim(F.col("account_id")) == ""),
    )
    df = (
        df
        .withColumn("open_date",          _parse_date("open_date"))
        .withColumn("last_activity_date", _parse_date("last_activity_date"))
        .withColumn("credit_limit",       F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance",    F.col("current_balance").cast(DecimalType(18, 2)))
    )
    for col in ("account_type", "account_status", "product_tier", "digital_channel"):
        df = df.withColumn(col, F.upper(F.trim(F.col(col))))
    df = _dedup(df, "account_id")
    logger.info("accounts Silver transform complete.")
    return df


# ── customers ─────────────────────────────────────────────────────────────────

def _transform_customers(bronze: DataFrame) -> DataFrame:
    logger.info("Transforming customers...")
    df = _drop_corrupt(bronze)
    df = (
        df
        .withColumn("dob",        _parse_date("dob"))
        .withColumn("risk_score", F.col("risk_score").cast(IntegerType()))
    )
    for col in ("gender", "province", "income_band", "segment", "kyc_status"):
        df = df.withColumn(col, F.trim(F.col(col)))
    df = _dedup(df, "customer_id")
    logger.info("customers Silver transform complete.")
    return df


# ── transactions ──────────────────────────────────────────────────────────────

def _flatten_transactions(df: DataFrame) -> DataFrame:
    if "location" in df.columns:
        df = (
            df
            .withColumn("location_province",   F.col("location.province"))
            .withColumn("location_city",        F.col("location.city"))
            .withColumn("location_coordinates", F.col("location.coordinates"))
            .drop("location")
        )
    else:
        df = (
            df
            .withColumn("location_province",   F.lit(None).cast("string"))
            .withColumn("location_city",        F.lit(None).cast("string"))
            .withColumn("location_coordinates", F.lit(None).cast("string"))
        )

    if "metadata" in df.columns:
        df = (
            df
            .withColumn("metadata_device_id",  F.col("metadata.device_id"))
            .withColumn("metadata_session_id", F.col("metadata.session_id"))
            .withColumn("metadata_retry_flag", F.col("metadata.retry_flag"))
            .drop("metadata")
        )
    else:
        df = (
            df
            .withColumn("metadata_device_id",  F.lit(None).cast("string"))
            .withColumn("metadata_session_id", F.lit(None).cast("string"))
            .withColumn("metadata_retry_flag", F.lit(None).cast("boolean"))
        )

    if "merchant_subcategory" not in df.columns:
        df = df.withColumn("merchant_subcategory", F.lit(None).cast("string"))

    return df


def _normalise_currency(df: DataFrame) -> DataFrame:
    zar_variants = ["ZAR", "zar", "R", "rands", "Rands", "RANDS", "710"]
    is_variant = (
        F.col("currency").isNotNull()
        & ~F.col("currency").isin(["ZAR"])
        & F.col("currency").isin(zar_variants)
    )
    df = (
        df
        .withColumn(
            "dq_flag",
            F.when(is_variant, F.lit("CURRENCY_VARIANT"))
             .otherwise(F.col("dq_flag") if "dq_flag" in df.columns else F.lit(None).cast("string")),
        )
        .withColumn(
            "currency",
            F.when(F.col("currency").isin(zar_variants), F.lit("ZAR"))
             .otherwise(F.col("currency")),
        )
    )
    return df


def _transform_transactions(bronze: DataFrame) -> DataFrame:
    logger.info("Transforming transactions...")
    df = _drop_corrupt(bronze)
    df = _flatten_transactions(df)
    df = df.withColumn("dq_flag", F.lit(None).cast("string"))
    df = _normalise_currency(df)
    df = df.withColumn("transaction_date", _parse_date("transaction_date"))
    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.concat_ws(" ",
                        F.date_format(F.col("transaction_date"), "yyyy-MM-dd"),
                        F.col("transaction_time")),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )
    df = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
    for col in ("transaction_type", "currency", "channel"):
        if col in df.columns:
            df = df.withColumn(col, F.upper(F.trim(F.col(col).cast("string"))))
    df = _dedup(df, "transaction_id")
    logger.info("transactions Silver transform complete.")
    return df


def _validate_account_customer_fk(
    accounts_df: DataFrame,
    customers_df: DataFrame,
) -> DataFrame:
    cust_ids = F.broadcast(
        customers_df.select(F.col("customer_id").alias("_cid"))
    )
    return (
        accounts_df
        .join(cust_ids, accounts_df["customer_ref"] == cust_ids["_cid"], "left")
        .withColumn("_dq_orphan_account", F.col("_cid").isNull())
        .drop("_cid")
    )


# ── Public entry point ────────────────────────────────────────────────────────

def run_transformation(spark: SparkSession = None, cfg: dict = None) -> None:
    _own_spark = spark is None
    if cfg is None:
        cfg = load_config()
    if spark is None:
        spark = get_spark(cfg)

    bronze_base = cfg["output"]["bronze_path"]
    silver_base = cfg["output"]["silver_path"]

    try:
        logger.info("Reading Bronze layers...")
        accounts_b     = read_delta(spark, f"{bronze_base}/accounts")
        customers_b    = read_delta(spark, f"{bronze_base}/customers")
        transactions_b = read_delta(spark, f"{bronze_base}/transactions")

        accounts_s     = _transform_accounts(accounts_b)
        customers_s    = _transform_customers(customers_b)
        transactions_s = _transform_transactions(transactions_b)

        accounts_s = _validate_account_customer_fk(accounts_s, customers_s)

        logger.info("Writing Silver layers...")
        write_delta(accounts_s,     f"{silver_base}/accounts")
        write_delta(customers_s,    f"{silver_base}/customers")
        write_delta(transactions_s, f"{silver_base}/transactions")

        logger.info("Silver transformation complete.")
    except Exception as exc:
        logger.exception("Silver transformation failed: %s", exc)
        if _own_spark:
            spark.stop()
        sys.exit(1)
    finally:
        if _own_spark:
            spark.stop()


if __name__ == "__main__":
    run_transformation()
