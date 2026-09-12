"""
Retail POS Daily Ingestion & Aggregation Pipeline
"""
import csv
import logging
from pathlib import Path
import sqlite3
from typing import List, Dict, Any, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

def ingest_csv(filepath: str) -> List[Dict[str, Any]]:
    """
    Reads a raw CSV file and returns a list of dictionaries with basic type casting.
    """
    path = Path(filepath)
    # Handle file not found
    if not path.exists():
        logger.error(f"File not found at {filepath}")
        raise FileNotFoundError(f"File missing: {filepath}")

    # Handle empty file
    if path.stat().st_size == 0:
        logger.warning(f"File is empty: {filepath}")
        return []

    records = []
    with open(path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=1):
            try:
                record = {
                    "txn_id": str(row["txn_id"]).strip(),
                    "store_id": str(row["store_id"]).strip(),
                    "txn_timestamp": str(row["txn_timestamp"]).strip(),
                    "product_sku": str(row["product_sku"]).strip(),
                    "quantity": int(row["quantity"]),
                    "unit_price": float(row["unit_price"]),
                    "discount_pct": float(row["discount_pct"])
                }
                records.append(record)
            except (KeyError, ValueError) as e:
                logger.warning(f"Skipping malformed record {row_num}. Error: {e}")

    logger.info(f"Successfully ingested {len(records)} records from {filepath}.")
    return records

def clean_transactions(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Deduplicates records by txn_id and filters out invalid
    quantities, prices, and discounts.

    Returns:
        (clean_records, quarantined_records)
    """
    clean_records = []
    quarantined = []
    seen_ids = set()
    for record in records:
        record_id = record["txn_id"]
        # Deduplicate
        if record_id in seen_ids:
            logger.warning(f"Duplicate transaction skipped: {record_id}")
            continue

        seen_ids.add(record_id)
        # Validate transaction values
        if (record["quantity"] > 0 and record["unit_price"] > 0 and 0 <= record["discount_pct"] <= 100):
            # Calculate gross amount
            record["gross_amount"] = (record["quantity"] * record["unit_price"])
            # Calculate net amount after discount
            record["net_amount"] = (record["gross_amount"] * (1 - record["discount_pct"] / 100.0))
            clean_records.append(record)
        else:
            quarantined.append(record)

    logger.info(f"Clean records: {len(clean_records)}, "f"Quarantined records: {len(quarantined)}")
    return clean_records, quarantined

def init_db(db_path: str = "retail_sales.db",schema_file: str = "schema.sql"):
    """
    Initializes the SQLite database using schema.sql.
    """
    with open(schema_file, "r", encoding="utf-8") as f:
        ddl = f.read()
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.executescript(ddl)
        conn.commit()
    finally:
        conn.close()
    logger.info("Database schema successfully initialized.")

def load_clean_transactions(db_conn: sqlite3.Connection,records: List[Dict[str, Any]]) -> int:
    """
    Loads clean transactions into stg_pos_transactions idempotently.
    Uses ON CONFLICT(txn_id) DO UPDATE so that rerunning the same
    records does not create duplicates.
    Returns:
        Number of successfully upserted records.
    """

    upsert_sql = """
        INSERT INTO stg_pos_transactions (
            txn_id,
            store_id,
            txn_timestamp,
            product_sku,
            quantity,
            unit_price,
            discount_pct,
            net_amount
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(txn_id) DO UPDATE SET
            store_id = excluded.store_id,
            txn_timestamp = excluded.txn_timestamp,
            product_sku = excluded.product_sku,
            quantity = excluded.quantity,
            unit_price = excluded.unit_price,
            discount_pct = excluded.discount_pct,
            net_amount = excluded.net_amount,
            ingested_at = CURRENT_TIMESTAMP
    """

    loaded_count = 0
    batch_size = 5
    cursor = db_conn.cursor()
    try:
        for batch in batch_generator(records, batch_size):
            cursor.executemany(upsert_sql, batch)
            db_conn.commit()
            loaded_count += len(batch)
            logger.info(f"Successfully upserted batch of {len(batch)} records.")
    except Exception as e:
        db_conn.rollback()
        logger.error(f"Exception occurred while loading transactions: {e}")
        raise
    finally:
        cursor.close()
    logger.info(f"Total transactions upserted: {loaded_count}")
    return loaded_count

def batch_generator(records, batch_size=5):
    """
    Generates records in batches.
    """
    batch = []
    for r in records:
        batch.append(
            (
                r["txn_id"],
                r["store_id"],
                r["txn_timestamp"],
                r["product_sku"],
                r["quantity"],
                r["unit_price"],
                r["discount_pct"],
                r["net_amount"]
            )
        )
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def generate_daily_mart(db_conn: sqlite3.Connection) -> int:
    """Aggregates staging transactions and updates the summary mart."""
    # We perform an idempotent upsert directly in one SQL statement
    mart_sql = """
    INSERT OR REPLACE INTO daily_store_sales_summary (
        report_date, store_id, total_transactions, total_items_sold, 
        gross_revenue, net_revenue, updated_at
    )
    SELECT 
        DATE(txn_timestamp) as report_date,
        store_id,
        COUNT(DISTINCT txn_id) as total_transactions,
        SUM(quantity) as total_items_sold,
        SUM(quantity * unit_price) as gross_revenue,
        SUM(net_amount) as net_revenue,
        CURRENT_TIMESTAMP
    FROM stg_pos_transactions
    GROUP BY 1, 2;
    """
    cursor = db_conn.cursor()
    try:
        cursor.execute(mart_sql)
        db_conn.commit()
        count = cursor.rowcount
        logger.info(f"Daily mart updated successfully. Records affected: {count}")
        return count
    except sqlite3.Error as e:
        db_conn.rollback()
        logger.error(f"Data mart generation failed: {e}")
        raise  

if __name__ == "__main__":
    try:
        # Initialize database
        init_db()
        # Create connection
        conn = sqlite3.connect("retail_sales.db")
        try:
            # Ingest
            raw_data = ingest_csv("data/raw_pos_transactions.csv")
            # Clean
            clean_data, quarantined_data = clean_transactions(raw_data)
            # Load
            loaded_count = load_clean_transactions(
                conn,
                clean_data
            )
            mart_loaded_count = generate_daily_mart(conn)
            logger.info(f"Pipeline completed successfully. Loaded: {loaded_count}, Quarantined: {len(quarantined_data)}")
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")