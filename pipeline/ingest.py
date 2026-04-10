from __future__ import annotations

"""
Bronze Layer Ingestion Module

Responsibilities:
- Read raw source datasets
- Preserve schema as-is (NO transformations)
- Add a single ingestion_timestamp (consistent per run)
- Write Delta Lake Bronze tables

Design Principles:
- No transformations in Bronze
- Deterministic ingestion timestamp
- Fully config-driven (no hardcoding)
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# =========================================================
# CONSTANTS
# =========================================================

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

def get_spark(app_name: str = "nedbank-bronze") -> SparkSession:
    """
    Create SparkSession with Delta support.

    IMPORTANT:
    - Uses Delta compatible with Spark 3.5
    - No external dependency conflicts
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
# HELPERS
# =========================================================

def ensure_output_dir(path: str) -> None:
    """
    Ensure output directory exists.

    Spark writes INTO this directory.
    """
    Path(path).mkdir(parents=True, exist_ok=True)


def add_ingestion_timestamp(df: DataFrame, ts: str) -> DataFrame:
    """
    Add ingestion_timestamp column.

    Same timestamp must be applied to all rows.
    """
    return df.withColumn(
        "ingestion_timestamp",
        F.to_timestamp(F.lit(ts))
    )


# =========================================================
# READERS
# =========================================================

def read_csv(spark: SparkSession, path: str) -> DataFrame:
    """
    Generic CSV reader (raw mode).

    - No schema inference
    - Preserve original structure
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(path)
    )


def read_json(spark: SparkSession, path: str) -> DataFrame:
    """
    JSON reader (for JSONL transactions).
    """
    return spark.read.json(path)


# =========================================================
# WRITER
# =========================================================

def write_delta(df: DataFrame, path: str) -> None:
    """
    Write DataFrame in Delta format.
    """
    ensure_output_dir(path)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(path)
    )


# =========================================================
# MAIN PIPELINE
# =========================================================

def run_ingestion() -> None:
    """
    Execute Bronze ingestion pipeline.

    Flow:
    1. Load config
    2. Initialize Spark
    3. Read raw data
    4. Add ingestion timestamp
    5. Write Delta Bronze tables
    """
    config = load_config()
    spark = get_spark()

    input_cfg = config["input"]
    bronze_base = config["output"]["bronze_path"]

    paths = {
        "accounts": f"{bronze_base}/accounts",
        "customers": f"{bronze_base}/customers",
        "transactions": f"{bronze_base}/transactions",
    }

    # Single timestamp per run (CRITICAL requirement)
    ingestion_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # -------------------------
    # ACCOUNTS
    # -------------------------
    df_accounts = read_csv(spark, input_cfg["accounts_path"])
    df_accounts = add_ingestion_timestamp(df_accounts, ingestion_ts)
    write_delta(df_accounts, paths["accounts"])

    # -------------------------
    # CUSTOMERS
    # -------------------------
    df_customers = read_csv(spark, input_cfg["customers_path"])
    df_customers = add_ingestion_timestamp(df_customers, ingestion_ts)
    write_delta(df_customers, paths["customers"])

    # -------------------------
    # TRANSACTIONS
    # -------------------------
    df_transactions = read_json(spark, input_cfg["transactions_path"])
    df_transactions = add_ingestion_timestamp(df_transactions, ingestion_ts)
    write_delta(df_transactions, paths["transactions"])

    spark.stop()


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    run_ingestion()
