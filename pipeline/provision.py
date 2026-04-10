from __future__ import annotations

"""
Gold layer provisioning module.

Responsibilities:
- Read Silver Delta tables
- Build Gold dimensional model
- Create:
  - dim_customers
  - dim_accounts
  - fact_transactions
- Write Gold Delta tables

Design principles:
- Deterministic surrogate keys
- Clean joins
- No duplicate business keys in dimensions
- Business-friendly normalization belongs in Gold
"""

import os
from pathlib import Path
from typing import Any, Dict

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


CONFIG_PATH_DEFAULT = "/data/config/pipeline_config.yaml"


# =========================================================
# CONFIG
# =========================================================

def load_config() -> Dict[str, Any]:
    """
    Load pipeline configuration.

    Priority:
    1. PIPELINE_CONFIG environment variable
    2. Default path
    """
    config_path = os.environ.get("PIPELINE_CONFIG", CONFIG_PATH_DEFAULT)

    if not Path(config_path).exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# =========================================================
# SPARK
# =========================================================

def get_spark(app_name: str = "nedbank-gold") -> SparkSession:
    """
    Create SparkSession with Delta support.

    Uses Delta compatible with Spark 3.5.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0"
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# =========================================================
# IO
# =========================================================

def ensure_output_dir(path: str) -> None:
    """
    Ensure output directory exists.
    """
    Path(path).mkdir(parents=True, exist_ok=True)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """
    Read Delta table from path.
    """
    return spark.read.format("delta").load(path)


def write_delta(df: DataFrame, path: str) -> None:
    """
    Write DataFrame as Delta table.
    """
    ensure_output_dir(path)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(path)
    )


# =========================================================
# HELPERS
# =========================================================

def add_stable_surrogate_key(df: DataFrame, natural_key_col: str, sk_col: str) -> DataFrame:
    """
    Add deterministic surrogate key using xxhash64 on natural key.

    This keeps keys stable across re-runs for the same input.
    """
    return df.withColumn(sk_col, F.xxhash64(F.col(natural_key_col)))


def derive_age_band(df: DataFrame) -> DataFrame:
    """
    Derive age_band from dob.

    Buckets:
    - 18-25
    - 26-35
    - 36-45
    - 46-55
    - 56-65
    - 65+

    Assumes dob is present as a date-like column in Silver customers.
    """
    age_years = F.floor(
        F.datediff(F.current_date(), F.to_date(F.col("dob"))) / F.lit(365.25)
    )

    return df.withColumn(
        "age_band",
        F.when(age_years >= 65, F.lit("65+"))
         .when(age_years >= 56, F.lit("56-65"))
         .when(age_years >= 46, F.lit("46-55"))
         .when(age_years >= 36, F.lit("36-45"))
         .when(age_years >= 26, F.lit("26-35"))
         .when(age_years >= 18, F.lit("18-25"))
         .otherwise(F.lit(None))
    )


def normalize_transaction_type(df: DataFrame) -> DataFrame:
    """
    Map raw transaction types to business-friendly categories.

    Mapping:
    - CREDIT   -> deposit
    - DEBIT    -> withdrawal
    - FEE      -> payment
    - REVERSAL -> transfer
    """
    return df.withColumn(
        "transaction_type",
        F.when(F.col("transaction_type") == "CREDIT", F.lit("deposit"))
         .when(F.col("transaction_type") == "DEBIT", F.lit("withdrawal"))
         .when(F.col("transaction_type") == "FEE", F.lit("payment"))
         .when(F.col("transaction_type") == "REVERSAL", F.lit("transfer"))
         .otherwise(F.lit("unknown"))
    )


# =========================================================
# BUILDERS
# =========================================================

def build_dim_customers(df_customers: DataFrame) -> DataFrame:
    """
    Build dim_customers from Silver customers.

    Expected output columns:
    - customer_sk
    - customer_id
    - gender
    - province
    - income_band
    - segment
    - risk_score
    - kyc_status
    - age_band
    """
    df = df_customers.dropDuplicates(["customer_id"])
    df = derive_age_band(df)
    df = add_stable_surrogate_key(df, "customer_id", "customer_sk")

    return df.select(
        "customer_sk",
        "customer_id",
        "gender",
        "province",
        "income_band",
        "segment",
        "risk_score",
        "kyc_status",
        "age_band",
    )


def build_dim_accounts(df_accounts: DataFrame) -> DataFrame:
    """
    Build dim_accounts from Silver accounts.

    Renames:
    - customer_ref -> customer_id

    Expected output columns:
    - account_sk
    - account_id
    - customer_id
    - account_type
    - account_status
    - open_date
    - product_tier
    - digital_channel
    - credit_limit
    - current_balance
    - last_activity_date
    """
    df = df_accounts.dropDuplicates(["account_id"])

    if "customer_ref" in df.columns:
        df = df.withColumnRenamed("customer_ref", "customer_id")

    df = add_stable_surrogate_key(df, "account_id", "account_sk")

#   return df.select(
#       "account_sk",
#       "account_id",
#       "customer_id",
#       "account_type",
#       "account_status",
#       "open_date",
#       "product_tier",
#       "digital_channel",
#       "credit_limit",
#       "current_balance",
#       "last_activity_date",
#   )

    return df.select(
        "account_sk",
        "account_id",
        "customer_id",
        "account_type",
        "account_status",
        "open_date",
        "product_tier",
        "digital_channel",
        F.col("credit_limit").cast("decimal(18,2)").alias("credit_limit"),
        F.col("current_balance").cast("decimal(18,2)").alias("current_balance"),
        "last_activity_date",
    )

def build_fact_transactions(
    df_transactions: DataFrame,
    dim_accounts: DataFrame,
    dim_customers: DataFrame,
) -> DataFrame:
    """
    Build fact_transactions from Silver transactions plus dimensions.

    Joins:
    - transactions.account_id -> dim_accounts.account_id
    - dim_accounts.customer_id -> dim_customers.customer_id

    Adds:
    - transaction_sk
    - account_sk
    - customer_sk

    Stage 1 note:
    - merchant_subcategory is not present in source; set to NULL
    """
    accounts_lookup = dim_accounts.select("account_id", "account_sk", "customer_id")
    customers_lookup = dim_customers.select("customer_id", "customer_sk", "province")

    df = df_transactions.dropDuplicates(["transaction_id"])

    # Business normalization belongs in Gold
    df = normalize_transaction_type(df)

    # Resolve dimensional keys
    df = df.join(accounts_lookup, on="account_id", how="inner")
    df = df.join(customers_lookup, on="customer_id", how="inner")

    # Deterministic fact surrogate key
    df = add_stable_surrogate_key(df, "transaction_id", "transaction_sk")

    # Stage 1 compatibility
    if "merchant_subcategory" not in df.columns:
        df = df.withColumn("merchant_subcategory", F.lit(None).cast("string"))

    return df.select(
        "transaction_sk",
        "transaction_id",
        "account_sk",
        "customer_sk",
        "transaction_date",
        "transaction_timestamp",
        "transaction_type",
        "merchant_category",
        "merchant_subcategory",
        "amount",
        "currency",
        "channel",
        "province",
        "dq_flag",
        "ingestion_timestamp",
    )


# =========================================================
# MAIN
# =========================================================

def run_provisioning() -> None:
    """
    Execute Gold layer provisioning.

    Steps:
    1. Load config
    2. Start Spark
    3. Read Silver tables
    4. Build dimensions
    5. Build fact table
    6. Write Gold Delta tables
    """
    config = load_config()
    spark = get_spark()

    silver_base = config["output"]["silver_path"]
    gold_base = config["output"]["gold_path"]

    silver_accounts_path = f"{silver_base}/accounts"
    silver_customers_path = f"{silver_base}/customers"
    silver_transactions_path = f"{silver_base}/transactions"

    gold_dim_accounts_path = f"{gold_base}/dim_accounts"
    gold_dim_customers_path = f"{gold_base}/dim_customers"
    gold_fact_transactions_path = f"{gold_base}/fact_transactions"

    df_accounts = read_delta(spark, silver_accounts_path)
    df_customers = read_delta(spark, silver_customers_path)
    df_transactions = read_delta(spark, silver_transactions_path)

    dim_customers = build_dim_customers(df_customers)
    dim_accounts = build_dim_accounts(df_accounts)
    fact_transactions = build_fact_transactions(df_transactions, dim_accounts, dim_customers)

    write_delta(dim_customers, gold_dim_customers_path)
    write_delta(dim_accounts, gold_dim_accounts_path)
    write_delta(fact_transactions, gold_fact_transactions_path)

    spark.stop()


if __name__ == "__main__":
    run_provisioning()
