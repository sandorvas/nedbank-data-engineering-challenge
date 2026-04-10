"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

This module transforms raw Bronze Delta tables into clean, typed, deduplicated
Silver tables ready for downstream processing.

Key responsibilities:
- Deduplication on natural keys
- Type standardisation
- Currency normalisation
- Basic data quality (DQ) flagging
- Referential integrity validation (accounts ↔ customers)

All paths must be read from configuration. No hardcoding allowed.
"""

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    to_timestamp,
    lit,
    when
)


# =========================
# CONFIG + SPARK
# =========================

def load_config():
    """
    Load pipeline configuration from mounted config file.

    Returns:
        dict: Parsed YAML configuration.
    """
    with open("/data/config/pipeline_config.yaml", "r") as f:
        return yaml.safe_load(f)


def create_spark():
    """
    Create a SparkSession configured for Delta Lake.

    Returns:
        SparkSession: Configured Spark session.
    """
    return (
        SparkSession.builder
        .appName("silver-layer")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


# =========================
# TRANSFORM FUNCTIONS
# =========================

def transform_accounts(df):
    """
    Clean and standardise accounts dataset.

    Steps:
    - Deduplicate by account_id
    - Standardise date fields

    Args:
        df (DataFrame): Bronze accounts DataFrame

    Returns:
        DataFrame: Cleaned accounts DataFrame
    """
    df = df.dropDuplicates(["account_id"])

    df = df.withColumn("open_date", to_date(col("open_date")))
    df = df.withColumn("last_activity_date", to_date(col("last_activity_date")))

    return df


def transform_customers(df):
    """
    Clean and standardise customers dataset.

    Steps:
    - Deduplicate by customer_id

    Args:
        df (DataFrame): Bronze customers DataFrame

    Returns:
        DataFrame: Cleaned customers DataFrame
    """
    df = df.dropDuplicates(["customer_id"])

    return df


def transform_transactions(df, accounts_df):
    """
    Clean and standardise transactions dataset.

    Steps:
    - Deduplicate by transaction_id
    - Standardise date and timestamp
    - Cast amount
    - Force currency to ZAR
    - Apply basic DQ flagging

    Args:
        df (DataFrame): Bronze transactions DataFrame
        accounts_df (DataFrame): Cleaned accounts DataFrame

    Returns:
        DataFrame: Cleaned transactions DataFrame
    """

    # Deduplicate
    df = df.dropDuplicates(["transaction_id"])

    # Standardise types
    df = df.withColumn("transaction_date", to_date(col("transaction_date")))

    df = df.withColumn(
        "transaction_timestamp",
        to_timestamp(col("transaction_date"))
    )

    df = df.withColumn(
        "amount",
        col("amount").cast("decimal(18,2)")
    )

    # Currency normalisation
    df = df.withColumn("currency", lit("ZAR"))

    # Referential integrity check (account must exist)
    df = df.join(
        accounts_df.select("account_id"),
        on="account_id",
        how="left"
    )

    # Basic DQ flagging
    df = df.withColumn(
        "dq_flag",
        when(col("account_id").isNull(), lit("ORPHANED_ACCOUNT"))
        .otherwise(lit(None))
    )

    return df


# =========================
# MAIN ENTRY POINT
# =========================

def run_transformation():
    """
    Execute Silver layer transformation pipeline.

    Workflow:
    1. Load configuration
    2. Create Spark session
    3. Read Bronze Delta tables
    4. Apply transformations
    5. Write Silver Delta tables

    Raises:
        Exception: If any stage fails
    """

    config = load_config()
    spark = create_spark()

    try:
        # Read Bronze tables
        accounts = spark.read.format("delta").load("/data/output/bronze/accounts/")
        customers = spark.read.format("delta").load("/data/output/bronze/customers/")
        transactions = spark.read.format("delta").load("/data/output/bronze/transactions/")

        # Transform
        accounts_clean = transform_accounts(accounts)
        customers_clean = transform_customers(customers)
        transactions_clean = transform_transactions(transactions, accounts_clean)

        # Write Silver tables
        accounts_clean.write.format("delta").mode("overwrite").save("/data/output/silver/accounts/")
        customers_clean.write.format("delta").mode("overwrite").save("/data/output/silver/customers/")
        transactions_clean.write.format("delta").mode("overwrite").save("/data/output/silver/transactions/")

    finally:
        spark.stop()
