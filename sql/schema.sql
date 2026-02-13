PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY,
    order_date TEXT UNIQUE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    quarter TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_region (
    region_id INTEGER PRIMARY KEY,
    region_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id INTEGER PRIMARY KEY,
    channel_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customer_segment (
    segment_id INTEGER PRIMARY KEY,
    segment_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    UNIQUE (product_name, category_id),
    FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    order_id INTEGER PRIMARY KEY,
    date_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    segment_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    units_sold INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    discount_pct REAL NOT NULL,
    gross_revenue REAL NOT NULL,
    net_revenue REAL NOT NULL,
    cost REAL NOT NULL,
    profit REAL NOT NULL,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (region_id) REFERENCES dim_region(region_id),
    FOREIGN KEY (channel_id) REFERENCES dim_channel(channel_id),
    FOREIGN KEY (segment_id) REFERENCES dim_customer_segment(segment_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_date_id ON fact_sales(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_region_id ON fact_sales(region_id);
CREATE INDEX IF NOT EXISTS idx_fact_channel_id ON fact_sales(channel_id);
CREATE INDEX IF NOT EXISTS idx_fact_product_id ON fact_sales(product_id);