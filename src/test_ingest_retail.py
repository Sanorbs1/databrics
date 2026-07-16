"""
Pytest tests for retail ingestion module.

Run in Databricks:
- Use the Testing sidebar to discover and run tests, or
- From a notebook cell:
  %sh
  pytest test_ingest_retail.py -v
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingest_retail import (
    IngestionError,
    clean_orders,
    ingest_orders,
    get_spark,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide the active Spark session (Databricks or local)."""
    spark = get_spark()
    return spark


def make_orders_df(spark: SparkSession, rows: list[tuple]) -> "DataFrame":
    """Helper to create a small orders DataFrame for tests."""
    schema = "order_id INT, product_id STRING, quantity INT, price DOUBLE, order_date STRING, store_id STRING"
    return spark.createDataFrame(rows, schema=schema)


def test_clean_orders_filters_invalid_rows(spark: SparkSession) -> None:
    """
    Verify that clean_orders:
    - Drops rows with quantity <= 0 or price <= 0
    - Keeps valid rows
    - Adds total_amount correctly.
    """
    rows = [
        (1001, "P001", 2, 19.99, "2024-01-01", "StoreA"),   # valid
        (1002, "P002", 0, 49.50, "2024-01-01", "StoreB"),   # quantity=0 -> drop
        (1003, "P003", -1, 9.75, "2024-01-02", "StoreC"),   # negative qty -> drop
        (1004, "P004", 3, 0.0, "2024-01-02", "StoreD"),     # price=0 -> drop
        (1005, "P005", 1, 9.99, "2024-01-03", "StoreE"),    # valid
    ]
    df = make_orders_df(spark, rows)

    cleaned = clean_orders(df)

    assert cleaned.count() == 2

    collected = cleaned.select("order_id", "quantity", "price", "total_amount").collect()
    collected_sorted = sorted(collected, key=lambda r: r.order_id)

    r1 = collected_sorted[0]
    r2 = collected_sorted[1]

    assert r1.order_id in (1001, 1005)
    assert r2.order_id in (1001, 1005)

    # Check total_amount logic for one row
    row_1001 = [r for r in collected_sorted if r.order_id == 1001][0]
    assert row_1001.quantity == 2
    assert abs(row_1001.price - 19.99) < 1e-6
    assert abs(row_1001.total_amount - (2 * 19.99)) < 1e-6


def test_clean_orders_missing_columns_raises(spark: SparkSession) -> None:
    """
    Ensure clean_orders raises IngestionError when required columns are missing.
    """
    # Create a DataFrame missing 'price'
    rows = [
        (1001, "P001", 2, "2024-01-01", "StoreA"),
    ]
    schema = "order_id INT, product_id STRING, quantity INT, order_date STRING, store_id STRING"
    df = spark.createDataFrame(rows, schema=schema)

    with pytest.raises(IngestionError, match="Missing required columns"):
        clean_orders(df)


def test_ingest_orders_writes_delta_table(spark: SparkSession) -> None:
    """
    End-to-end test: ingest a small CSV-like dataset into a temporary Delta table.

    This test:
    - Creates a tiny in-memory CSV via DataFrame
    - Writes it as CSV to a temp path (or uses a static small file in your repo)
    - Calls ingest_orders
    - Verifies the Delta table exists and has expected row count.
    """
    import tempfile
    import os

    # Create a temporary CSV file with sample retail data
    temp_dir = tempfile.mkdtemp()
    csv_path = os.path.join(temp_dir, "orders_test.csv")

    with open(csv_path, "w") as f:
        f.write("order_id,product_id,quantity,price,order_date,store_id\n")
        f.write("2001,P001,2,19.99,2024-01-01,StoreA\n")
        f.write("2002,P002,1,49.50,2024-01-01,StoreB\n")
        f.write("2003,P003,0,9.75,2024-01-02,StoreC\n")  # will be filtered out

    catalog = "main"
    schema = "retail"
    table = "orders_test_pytest"
    full_name = f"{catalog}.{schema}.{table}"

    # Clean up previous test runs
    if spark.catalog.tableExists(full_name):
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")

    try:
        result_table = ingest_orders(csv_path, catalog, schema, table, mode="overwrite")
        assert result_table == full_name

        assert spark.catalog.tableExists(full_name)

        df = spark.table(full_name)
        # Only 2 rows should survive cleaning (2001 and 2002)
        assert df.count() == 2

        # Basic sanity: total_amount exists and is positive
        assert "total_amount" in df.columns
        assert df.filter(F.col("total_amount") <= 0).count() == 0

    finally:
        # Optionally drop the test table after the test
        if spark.catalog.tableExists(full_name):
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    csv_path = "/dbfs/FileStore/shared_uploads/retail_orders.csv
        # Clean up temp CSV
        if os.path.exists(csv_path):
            os.remove(csv_path)