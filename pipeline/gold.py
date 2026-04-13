"""
Gold Layer Pipeline (Delta Lake + Spark)

Builds dimensional models and an enriched fact table with
behavioral intelligence and risk scoring.

Features
--------
- Deterministic surrogate keys (xxhash64)
- Data quality validation
- Partitioned Delta tables
- DuckDB-compatible timestamps
- Intelligence signals:
    - high_value_flag
    - channel_risk
    - txn_hour
    - txn_per_hour
    - velocity_flag
    - risk_score (composite)

Architecture
------------
Bronze → Silver → Gold
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    countDistinct,
    current_timestamp,
    floor,
    hour,
    lit,
    months_between,
    to_timestamp,
    when,
    xxhash64,
)
from pyspark.sql.functions import count as spark_count
from pyspark.sql.window import Window

# =========================================================
# Config
# =========================================================
DATA_DIR = "../data"
OUTPUT_DIR = "../output/gold"
OUTPUT_FORMAT = "delta"

TRANSACTIONS_PATH = f"{DATA_DIR}/transactions.jsonl"
ACCOUNTS_PATH = f"{DATA_DIR}/accounts.csv"
CUSTOMERS_PATH = f"{DATA_DIR}/customers.csv"

FACT_PARTITIONS = ["province", "transaction_date"]

# --- thresholds ---
HIGH_VALUE_THRESHOLD = 10000
AMOUNT_MEDIUM_THRESHOLD = 2000
AMOUNT_HIGH_THRESHOLD = 5000

VELOCITY_MEDIUM_THRESHOLD = 2
VELOCITY_HIGH_THRESHOLD = 3

# =========================================================
# Spark
# =========================================================
def create_spark():
    builder = (
        SparkSession.builder
        .appName("gold-layer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


spark = create_spark()

# =========================================================
# Helpers
# =========================================================
def sk(col_name: str):
    return xxhash64(col(col_name)).cast("bigint")


def write_table(df, path: str, partition_cols=None):
    writer = (
        df.write
        .mode("overwrite")
        .format(OUTPUT_FORMAT)
        .option("overwriteSchema", "true")
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(path)

# =========================================================
# Load
# =========================================================
def load_data():
    return (
        spark.read.json(TRANSACTIONS_PATH),
        spark.read.csv(ACCOUNTS_PATH, header=True, inferSchema=True),
        spark.read.csv(CUSTOMERS_PATH, header=True, inferSchema=True),
    )

# =========================================================
# Clean
# =========================================================
def clean_transactions(df):
    return (
        df.dropDuplicates(["transaction_id"])
        .withColumn(
            "dq_flag",
            when(col("transaction_id").isNull(), "NULL_REQUIRED")
            .when(col("account_id").isNull(), "NULL_REQUIRED")
            .when(col("transaction_date").isNull(), "NULL_REQUIRED")
            .when(col("amount").isNull(), "NULL_REQUIRED")
            .when(
                col("currency").isNotNull() & (col("currency") != "ZAR"),
                "CURRENCY_VARIANT",
            )
            .otherwise(lit(None).cast("string"))
        )
    )

# =========================================================
# Dimensions
# =========================================================
def build_dim_customers(df, now_ts):
    return (
        df.dropDuplicates(["customer_id"])
        .withColumn(
            "age_years",
            floor(months_between(now_ts, col("dob")) / 12)
        )
        .select(
            sk("customer_id").alias("customer_sk"),
            "customer_id",
            "gender",
            "province",
            "income_band",
            "segment",
            col("risk_score").cast("int"),
            "kyc_status",
        )
    )


def build_dim_accounts(df):
    return (
        df.dropDuplicates(["account_id"])
        .select(
            sk("account_id").alias("account_sk"),
            "account_id",
            col("customer_ref").alias("customer_id"),
            "account_type",
            "account_status",
            col("open_date").cast("date"),
            "product_tier",
            "digital_channel",
        )
    )

# =========================================================
# Fact + Intelligence
# =========================================================
def build_fact(transactions, accounts_dim, customers_dim, now_ts):

    joined = (
        transactions
        .join(accounts_dim.select("account_sk", "account_id", "customer_id"), "account_id")
        .join(customers_dim.select("customer_sk", "customer_id"), "customer_id")
    )

    base = (
        joined.select(
            sk("transaction_id").alias("transaction_sk"),
            "transaction_id",
            "account_sk",
            "customer_sk",
            col("transaction_date").cast("date"),
            to_timestamp(
                concat_ws(" ", col("transaction_date"), col("transaction_time"))
            ).cast("timestamp_ntz").alias("transaction_timestamp"),
            "transaction_type",
            "merchant_category",
            col("amount").cast("decimal(18,2)"),
            "channel",
            col("location.province").alias("province"),
            now_ts.cast("timestamp_ntz").alias("ingestion_timestamp"),
        )
    )

    enriched = (
        base
        .withColumn(
            "high_value_flag",
            when(col("amount") > HIGH_VALUE_THRESHOLD, "HIGH").otherwise("NORMAL")
        )
        .withColumn(
            "channel_risk",
            when(col("channel").isin("ATM", "USSD"), "HIGH")
            .when(col("channel") == "POS", "MEDIUM")
            .otherwise("LOW")
        )
        .withColumn("txn_hour", hour(col("transaction_timestamp")))
    )

    window = Window.partitionBy("customer_sk", "transaction_date", "txn_hour")

    enriched = enriched.withColumn(
        "txn_per_hour",
        spark_count("*").over(window)
    )

    enriched = enriched.withColumn(
        "velocity_flag",
        when(col("txn_per_hour") > VELOCITY_HIGH_THRESHOLD, "HIGH")
        .when(col("txn_per_hour") > VELOCITY_MEDIUM_THRESHOLD, "MEDIUM")
        .otherwise("LOW")
    )

    # --- composite risk score ---
    enriched = enriched.withColumn(
        "risk_score",
        (
            when(col("velocity_flag") == "HIGH", 2)
            .when(col("velocity_flag") == "MEDIUM", 1)
            .otherwise(0)
        )
        +
        (
            when(col("amount") > AMOUNT_HIGH_THRESHOLD, 2)
            .when(col("amount") > AMOUNT_MEDIUM_THRESHOLD, 1)
            .otherwise(0)
        )
        +
        (
            when(col("channel_risk") == "HIGH", 1)
            .when(col("channel_risk") == "MEDIUM", 0.5)
            .otherwise(0)
        )
    )

    return enriched.dropDuplicates(["transaction_id"])

# =========================================================
# Validation
# =========================================================
def validate(df, name: str):
    sk_col = {
        "fact": "transaction_sk",
        "accounts": "account_sk",
        "customers": "customer_sk",
    }[name]

    row = df.select(
        count(lit(1)).alias("rows"),
        (count(sk_col) - countDistinct(sk_col)).alias("duplicates"),
        count(when(col(sk_col).isNull(), 1)).alias("null_keys"),
    ).collect()[0]

    print(f"\n🔎 {name}: {row['rows']} rows | dup={row['duplicates']} | null={row['null_keys']}")

# =========================================================
# Main
# =========================================================
def main():
    print("🚀 Gold pipeline")

    now_ts = current_timestamp()

    t, a, c = load_data()
    t = clean_transactions(t)

    dc = build_dim_customers(c, now_ts)
    da = build_dim_accounts(a)
    ft = build_fact(t, da, dc, now_ts)

    validate(dc, "customers")
    validate(da, "accounts")
    validate(ft, "fact")

    write_table(dc, f"{OUTPUT_DIR}/dim_customers")
    write_table(da, f"{OUTPUT_DIR}/dim_accounts")
    write_table(ft, f"{OUTPUT_DIR}/fact_transactions", FACT_PARTITIONS)

    print("✅ Done")


if __name__ == "__main__":
    main()
