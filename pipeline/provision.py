"""
pipeline/provision.py  ─  Gold Layer  (Silver → Gold)

Key optimisations vs prior version:
  • dim tables cached and materialised ONCE with a single .count() each.
    (Prior version called .count() twice — materialise + log.)
  • fact_transactions is cached before validation AND before write.
    This means the 1M-row plan is evaluated exactly ONCE total.
    The cache is released immediately after the write to free memory.
  • Validation runs AFTER writing Gold (not before), using the cached DataFrames
    so no re-reads are needed and no redundant fact evaluation occurs.
  • Called with an existing SparkSession — does NOT create or stop Spark.
"""
from __future__ import annotations

import sys
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

from pipeline.utils import (
    add_surrogate_key,
    get_logger, get_spark, load_config,
    read_delta, write_delta,
)

logger = get_logger("gold.provision")


# ── age_band ──────────────────────────────────────────────────────────────────

def _derive_age_band(df: DataFrame, dob_col: str = "dob") -> DataFrame:
    run_date_lit = F.lit(date.today().isoformat()).cast("date")
    age_expr = F.floor(F.datediff(run_date_lit, F.col(dob_col)) / F.lit(365.25))
    age_band_expr = (
        F.when(age_expr >= 65, F.lit("65+"))
         .when(age_expr >= 56, F.lit("56-65"))
         .when(age_expr >= 46, F.lit("46-55"))
         .when(age_expr >= 36, F.lit("36-45"))
         .when(age_expr >= 26, F.lit("26-35"))
         .when(age_expr >= 18, F.lit("18-25"))
         .otherwise(F.lit(None).cast("string"))
    )
    return df.withColumn("age_band", age_band_expr)


# ── dim_customers ─────────────────────────────────────────────────────────────

def _build_dim_customers(customers_s: DataFrame) -> DataFrame:
    logger.info("Building dim_customers...")
    df = customers_s.filter(
        F.col("customer_id").isNotNull()
        & (F.trim(F.col("customer_id")) != "")
    )
    df = _derive_age_band(df, "dob")
    df = add_surrogate_key(df, "customer_sk", "customer_id")
    df = df.withColumn("risk_score", F.col("risk_score").cast(IntegerType()))
    return df.select(
        "customer_sk", "customer_id", "gender", "province",
        "income_band", "segment", "risk_score", "kyc_status", "age_band",
    )


# ── dim_accounts ──────────────────────────────────────────────────────────────

def _build_dim_accounts(
    accounts_s: DataFrame,
    dim_customers: DataFrame,
) -> DataFrame:
    logger.info("Building dim_accounts...")
    df = accounts_s.filter(
        F.col("account_id").isNotNull()
        & (F.trim(F.col("account_id")) != "")
    )
    df = df.withColumnRenamed("customer_ref", "customer_id")
    valid_customers = F.broadcast(dim_customers.select("customer_id"))
    df = df.join(valid_customers, on="customer_id", how="inner")
    df = add_surrogate_key(df, "account_sk", "account_id")
    df = (
        df
        .withColumn("credit_limit",    F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance", F.col("current_balance").cast(DecimalType(18, 2)))
    )
    return df.select(
        "account_sk", "account_id", "customer_id", "account_type",
        "account_status", "open_date", "product_tier", "digital_channel",
        "credit_limit", "current_balance", "last_activity_date",
    )


# ── fact_transactions ─────────────────────────────────────────────────────────

def _build_fact_transactions(
    transactions_s: DataFrame,
    dim_accounts: DataFrame,
    dim_customers: DataFrame,
) -> DataFrame:
    logger.info("Building fact_transactions...")
    df = transactions_s.filter(
        F.col("transaction_id").isNotNull()
        & (F.trim(F.col("transaction_id")) != "")
    )

    acct_map = F.broadcast(dim_accounts.select("account_id", "account_sk", "customer_id"))
    df = df.join(acct_map, on="account_id", how="left")

    cust_map = F.broadcast(
        dim_customers.select(
            F.col("customer_id").alias("_cust_id"),
            F.col("customer_sk"),
        )
    )
    df = (
        df
        .join(cust_map, df["customer_id"] == cust_map["_cust_id"], "left")
        .drop("_cust_id")
    )

    df = df.withColumn(
        "dq_flag",
        F.when(
            F.col("account_sk").isNull() & F.col("dq_flag").isNull(),
            F.lit("ORPHANED_ACCOUNT"),
        ).otherwise(F.col("dq_flag")),
    )

    df = add_surrogate_key(df, "transaction_sk", "transaction_id")

    df = (
        df
        .withColumn("account_sk",  F.coalesce(F.col("account_sk"),  F.lit(0).cast("long")))
        .withColumn("customer_sk", F.coalesce(F.col("customer_sk"), F.lit(0).cast("long")))
    )

    province_col = "location_province" if "location_province" in df.columns else "province"

    df = df.withColumn(
        "ingestion_timestamp",
        F.to_timestamp(F.col("_ingestion_timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
    )
    df = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))

    return df.select(
        F.col("transaction_sk"),
        F.col("transaction_id"),
        F.col("account_sk"),
        F.col("customer_sk"),
        F.col("transaction_date"),
        F.col("transaction_timestamp"),
        F.col("transaction_type"),
        F.col("merchant_category"),
        F.col("merchant_subcategory"),
        F.col("amount"),
        F.col("currency"),
        F.col("channel"),
        F.col(province_col).alias("province"),
        F.col("dq_flag"),
        F.col("ingestion_timestamp"),
    )


# ── Validation ────────────────────────────────────────────────────────────────

def _run_validation(spark: SparkSession) -> bool:
    """
    Runs against temp views — no disk reads.
    Views must be registered before calling this.
    """
    checks = [
        (
            "V1 – 4 transaction types",
            "SELECT COUNT(DISTINCT transaction_type) AS n FROM fact_transactions",
            "n", 4, False,
        ),
        (
            "V2 – zero unlinked accounts",
            """
            SELECT COUNT(*) AS unlinked_accounts
            FROM dim_accounts a
            LEFT JOIN dim_customers c ON a.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
            """,
            "unlinked_accounts", 0, True,
        ),
        (
            "V3 – 9 provinces in dim_customers",
            "SELECT COUNT(DISTINCT province) AS n FROM dim_customers",
            "n", 9, False,
        ),
    ]

    passed = True
    for label, sql, col, expected, strict in checks:
        row    = spark.sql(sql).first()
        actual = row[col] if row else None
        ok     = (actual == expected) if strict else (actual >= expected)
        sym    = "✓" if ok else "✗"
        logger.info("%s %s  (expected %s %s, got %s)",
                    sym, label, "==" if strict else ">=", expected, actual)
        if not ok:
            passed = False

    return passed


# ── Public entry point ────────────────────────────────────────────────────────

def run_provisioning(spark: SparkSession = None, cfg: dict = None) -> None:
    """
    Gold provisioning.  Accepts an existing spark/cfg (shared-session mode)
    or bootstraps its own (standalone mode for backward-compat).

    Fact table evaluation count:
      OLD: 2× (once for temp view validation scan, once for write)
      NEW: 1× (cached before write; validation reads from cache; cache freed after write)
    """
    _own_spark = spark is None
    if cfg is None:
        cfg = load_config()
    if spark is None:
        spark = get_spark(cfg)

    silver_base = cfg["output"]["silver_path"]
    gold_base   = cfg["output"]["gold_path"]

    try:
        logger.info("Reading Silver layers...")
        customers_s    = read_delta(spark, f"{silver_base}/customers")
        accounts_s     = read_delta(spark, f"{silver_base}/accounts")
        transactions_s = read_delta(spark, f"{silver_base}/transactions")

        # ── Build and cache dims (small: ~5-10MB each).
        #    Single .count() call materialises the cache — no second call.
        logger.info("Building dim_customers...")
        dim_customers = _build_dim_customers(customers_s).cache()
        n_customers = dim_customers.count()   # ONE call: materialises + counts
        logger.info("dim_customers rows: %d", n_customers)

        logger.info("Building dim_accounts...")
        dim_accounts = _build_dim_accounts(accounts_s, dim_customers).cache()
        n_accounts = dim_accounts.count()     # ONE call: materialises + counts
        logger.info("dim_accounts rows: %d", n_accounts)

        # ── Build fact and cache it.
        #    Caching a 1M-row DataFrame (~50-80MB uncompressed with select) is
        #    acceptable within the 2GB budget when dims are already cached
        #    (~15MB total).  This ensures the fact is evaluated ONCE regardless
        #    of how many downstream operations consume it.
        logger.info("Building fact_transactions...")
        fact_txn = _build_fact_transactions(
            transactions_s, dim_accounts, dim_customers
        ).cache()
        n_fact = fact_txn.count()             # ONE call: materialises + counts
        logger.info("fact_transactions rows: %d", n_fact)

        # ── Validate before write — all from cache, zero disk reads
        dim_customers.createOrReplaceTempView("dim_customers")
        dim_accounts.createOrReplaceTempView("dim_accounts")
        fact_txn.createOrReplaceTempView("fact_transactions")

        passed = _run_validation(spark)
        if not passed:
            logger.error("Validation failed — aborting Gold write.")
            sys.exit(1)

        # ── Write Gold — all DataFrames served from cache
        logger.info("Writing Gold layer...")
        write_delta(dim_customers, f"{gold_base}/dim_customers")
        write_delta(dim_accounts,  f"{gold_base}/dim_accounts")
        write_delta(fact_txn,      f"{gold_base}/fact_transactions")

        # ── Release caches
        dim_customers.unpersist()
        dim_accounts.unpersist()
        fact_txn.unpersist()

        logger.info("Gold layer written successfully.")
        logger.info("All Gold validations passed. Pipeline complete ✓")

    except Exception as exc:
        logger.exception("Gold provisioning failed: %s", exc)
        if _own_spark:
            spark.stop()
        sys.exit(1)
    finally:
        if _own_spark:
            spark.stop()


if __name__ == "__main__":
    run_provisioning()
