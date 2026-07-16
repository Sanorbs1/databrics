"""
Retail order ingestion module for Databricks.

Functions:
- load_orders_csv: Load CSV into a Spark DataFrame.
- clean_orders: Apply basic validation and transformations.
- write_orders_delta: Write cleaned data to a Delta table.
- ingest_orders: End-to-end ingestion pipeline.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Dict, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import IllegalArgumentException  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)


class IngestionError(Exception):
    """Custom exception for retail ingestion failures."""


def get_spark() -> SparkSession:
    """Return the active Spark session (Databricks or local)."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise IngestionError("No active Spark session found.")
    return spark

path ="/Volumes/sanor/retail/retail_raw/order.csv.txt" 
def load_orders_csv(path: str) -> DataFrame:
    """
    Load retail orders from a CSV file into a Spark DataFrame.

    Args:
        path: File path (DBFS, Volume, or local) to the CSV.

    Returns:
        A Spark DataFrame with inferred schema.

    Raises:
        IngestionError: If the file cannot be read.
    """
    spark = get_spark()
    logger.info("Loading orders CSV from %s", path)

    try:
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path)
        )
        logger.info("Loaded %d rows from %s", df.count(), path)
        return df
    except Exception as e:
        logger.exception("Failed to load CSV from %s", path)
        raise IngestionError(f"Failed to load CSV from {path}") from e


def clean_orders(df: DataFrame) -> DataFrame:
    """
    Apply cleaning and validation rules to the orders DataFrame.

    Rules applied:
    - Ensure required columns exist: order_id, product_id, quantity, price, order_date, store_id.
    - Drop rows with null order_id or product_id.
    - Filter out rows where quantity <= 0 or price <= 0.
    - Cast quantity to int, price to double.
    - Add `total_amount` = quantity * price.

    Args:
        df: Raw orders DataFrame.

    Returns:
        Cleaned orders DataFrame.

    Raises:
        IngestionError: If required columns are missing.
    """
    required_cols = [
        "order_id",
        "product_id",
        "quantity",
        "price",
        "order_date",
        "store_id",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error("Missing required columns: %s", missing)
        raise IngestionError(f"Missing required columns: {missing}")

    logger.info("Starting cleaning: %d rows", df.count())

    cleaned = (
        df
        # Drop rows with null critical keys
        .filter(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())
        # Filter invalid quantity/price
        .filter((F.col("quantity") > 0) & (F.col("price") > 0))
        # Cast types
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("price", F.col("price").cast("double"))
        # Add derived column
        .withColumn("total_amount", F.col("quantity") * F.col("price"))
    )

    logger.info("After cleaning: %d rows", cleaned.count())
    return cleaned


def write_orders_delta(
    df: DataFrame,
    catalog: str,
    schema: str,
    table: str,
    mode: str = "overwrite",
) -> str:
    """
    Write cleaned orders DataFrame to a Delta table.

    Args:
        df: Cleaned orders DataFrame.
        catalog: Unity Catalog name.
        schema: Schema / database name.
        table: Table name.
        mode: Save mode ('overwrite' or 'append').

    Returns:
        The fully qualified table name.

    Raises:
        IngestionError: If writing fails.
    """
    full_name = f"{catalog}.{schema}.{table}"
    logger.info("Writing %d rows to Delta table %s (mode=%s)", df.count(), full_name, mode)

    try:
        (
            df.write.format("delta")
            .mode(mode)
            .saveAsTable(full_name)
        )
        logger.info("Successfully wrote to Delta table %s", full_name)
        return full_name
    except Exception as e:
        logger.exception("Failed to write Delta table %s", full_name)
        raise IngestionError(f"Failed to write Delta table {full_name}") from e


def ingest_orders(
    input_path: str,
    catalog: str,
    schema: str,
    table: str,
    mode: str = "overwrite",
) -> str:
    """
    End-to-end ingestion pipeline for retail orders.

    Steps:
    1. Load CSV from `input_path`.
    2. Clean/validate data.
    3. Write to Delta table `catalog.schema.table`.

    Args:
        input_path: Path to the input CSV file.
        catalog: Unity Catalog name.
        schema: Schema / database name.
        table: Table name.
        mode: Save mode for Delta write.

    Returns:
        Fully qualified Delta table name.

    Raises:
        IngestionError: On any ingestion failure.
    """
    logger.info("Starting ingestion from %s to %s.%s.%s", input_path, catalog, schema, table)

    try:
        raw_df = load_orders_csv(input_path)
        cleaned_df = clean_orders(raw_df)
        full_name = write_orders_delta(cleaned_df, catalog, schema, table, mode=mode)
        logger.info("Ingestion completed to %s", full_name)
        return full_name
    except IngestionError:
        # Re-raise as-is
        raise
    except Exception as e:
        logger.exception("Unexpected error during ingestion")
        raise IngestionError("Unexpected ingestion failure") from e


# Optional: a simple CLI-like entry point for notebooks
def run_ingestion_from_args(
    input_path: str,
    catalog: str,
    schema: str,
    table: str,
) -> None:
    """
    Run ingestion with logging configured, suitable for calling from a notebook cell.

    Args:
        input_path: Path to input CSV.
        catalog: Catalog name.
        schema: Schema name.
        table: Table name.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    try:
        ingest_orders(input_path, catalog, schema, table)
    except IngestionError:
        # In notebooks you might want to re-raise or handle differently
        raise