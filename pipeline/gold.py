from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    countDistinct,
    current_timestamp,
    floor,
    lit,
    months_between,
    to_timestamp,
    when,
    xxhash64,
)

# =========================================================
# Config
# =========================================================
DATA_DIR = "../data"
OUTPUT_DIR = "../output/gold"
OUTPUT_FORMAT = "delta"

TRANSACTIONS_PATH = f"{DATA_DIR}/transactions.jsonl"
ACCOUNTS_PATH = f"{DATA_DIR}/accounts.csv"
CUSTOMERS_PATH = f"{DATA_DIR}/customers.csv"

# =========================================================
# Spark Session
# =========================================================
def create_spark():
    """
    Create Spark session with Delta support and UTC timezone.
    """
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
    """Deterministic BIGINT surrogate key."""
    return xxhash64(col(col_name)).cast("bigint")


def write_table(df, path: str):
    """Write DataFrame safely (overwrite + schema evolution)."""
    (
        df.write
        .mode("overwrite")
        .format(OUTPUT_FORMAT)
        .option("overwriteSchema", "true")
        .save(path)
    )


# =========================================================
# Load
# =========================================================
def load_data():
    """Load raw datasets."""
    return (
        spark.read.json(TRANSACTIONS_PATH),
        spark.read.csv(ACCOUNTS_PATH, header=True, inferSchema=True),
        spark.read.csv(CUSTOMERS_PATH, header=True, inferSchema=True),
    )


# =========================================================
# DQ / Cleanup
# =========================================================
def clean_transactions(df):
    """Basic DQ rules."""
    return (
        df.dropDuplicates(["transaction_id"])
        .withColumn(
            "dq_flag",
            when(col("transaction_id").isNull(), lit("NULL_REQUIRED"))
            .when(col("account_id").isNull(), lit("NULL_REQUIRED"))
            .when(col("transaction_date").isNull(), lit("NULL_REQUIRED"))
            .when(col("amount").isNull(), lit("NULL_REQUIRED"))
            .when(
                col("currency").isNotNull() & (col("currency") != "ZAR"),
                lit("CURRENCY_VARIANT"),
            )
            .otherwise(lit(None).cast("string"))
        )
    )


# =========================================================
# Dimensions
# =========================================================
def build_dim_customers(df):
    """Customer dimension."""
    return (
        df.dropDuplicates(["customer_id"])
        .withColumn(
            "age_years",
            floor(months_between(current_timestamp(), col("dob")) / 12)
        )
        .withColumn(
            "age_band",
            when(col("age_years") >= 65, "65+")
            .when(col("age_years") >= 56, "56-65")
            .when(col("age_years") >= 46, "46-55")
            .when(col("age_years") >= 36, "36-45")
            .when(col("age_years") >= 26, "26-35")
            .when(col("age_years") >= 18, "18-25")
            .otherwise(lit(None).cast("string"))
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
            "age_band",
        )
    )


def build_dim_accounts(df):
    """Account dimension."""
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
            col("credit_limit").cast("decimal(18,2)"),
            col("current_balance").cast("decimal(18,2)"),
            col("last_activity_date").cast("date"),
        )
    )


# =========================================================
# Fact
# =========================================================
def build_fact(transactions, accounts_dim, customers_dim):
    """Fact table."""
    joined = (
        transactions
        .join(accounts_dim.select("account_sk", "account_id", "customer_id"), "account_id")
        .join(customers_dim.select("customer_sk", "customer_id"), "customer_id")
    )

    return (
        joined.select(
            sk("transaction_id").alias("transaction_sk"),
            "transaction_id",
            "account_sk",
            "customer_sk",
            col("transaction_date").cast("date"),

            # 🔥 FIX: remove timezone
            to_timestamp(
                concat_ws(" ", col("transaction_date"), col("transaction_time"))
            ).cast("timestamp_ntz").alias("transaction_timestamp"),

            "transaction_type",
            "merchant_category",
            lit(None).cast("string").alias("merchant_subcategory"),
            col("amount").cast("decimal(18,2)"),
            lit("ZAR").alias("currency"),
            "channel",
            col("location.province").alias("province"),
            "dq_flag",

            # 🔥 FIX: remove timezone
            current_timestamp().cast("timestamp_ntz").alias("ingestion_timestamp"),
        )
        .dropDuplicates(["transaction_id"])
    )


# =========================================================
# Validation
# =========================================================
def validate(df, name: str):
    """Basic validation."""
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

    print(f"\n🔎 Validation: {name}")
    print(f"Rows: {row['rows']}")
    print(f"Duplicates: {row['duplicates']}")
    print(f"Null keys: {row['null_keys']}")


# =========================================================
# Main
# =========================================================
def main():
    print("🚀 Starting Gold pipeline...")

    transactions, accounts, customers = load_data()
    transactions = clean_transactions(transactions)

    dim_customers = build_dim_customers(customers)
    dim_accounts = build_dim_accounts(accounts)
    fact_transactions = build_fact(transactions, dim_accounts, dim_customers)

    validate(dim_customers, "customers")
    validate(dim_accounts, "accounts")
    validate(fact_transactions, "fact")

    write_table(dim_customers, f"{OUTPUT_DIR}/dim_customers")
    write_table(dim_accounts, f"{OUTPUT_DIR}/dim_accounts")
    write_table(fact_transactions, f"{OUTPUT_DIR}/fact_transactions")

    print("\n✅ Pipeline completed successfully")


if __name__ == "__main__":
    main()
