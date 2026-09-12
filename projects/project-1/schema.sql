CREATE TABLE IF NOT EXISTS stg_pos_transactions (
    txn_id VARCHAR(50) NOT NULL PRIMARY KEY,
    store_id VARCHAR(20) NOT NULL,
    txn_timestamp TIMESTAMP NOT NULL,
    product_sku VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    discount_pct NUMERIC(5,2) DEFAULT 0.0,
    net_amount NUMERIC(10,2) NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_store_sales_summary (
    report_date DATE NOT NULL,
    store_id VARCHAR(20) NOT NULL,
    total_transactions INTEGER NOT NULL,
    total_items_sold INTEGER NOT NULL,
    gross_revenue NUMERIC(12,2) NOT NULL,
    net_revenue NUMERIC(12,2) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, store_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_store_sales_summary_store_id
ON daily_store_sales_summary (store_id);