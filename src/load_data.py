import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_FILE = ROOT_DIR / "data" / "raw" / "sales_transactions.csv"
WAREHOUSE_DIR = ROOT_DIR / "data" / "warehouse"
DB_PATH = WAREHOUSE_DIR / "sales.db"
SCHEMA_PATH = ROOT_DIR / "sql" / "schema.sql"


def build_dimension(source: pd.Series, id_col: str, name_col: str) -> pd.DataFrame:
    values = sorted(source.dropna().unique().tolist())
    return pd.DataFrame({id_col: range(1, len(values) + 1), name_col: values})


def create_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    date_values = pd.to_datetime(df["order_date"]).drop_duplicates().sort_values()
    dim_date = pd.DataFrame({"order_date": date_values})
    dim_date["date_id"] = dim_date["order_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["order_date"].dt.year.astype(int)
    dim_date["month"] = dim_date["order_date"].dt.month.astype(int)
    dim_date["month_name"] = dim_date["order_date"].dt.strftime("%b")
    dim_date["quarter"] = "Q" + dim_date["order_date"].dt.quarter.astype(str)
    dim_date["order_date"] = dim_date["order_date"].dt.strftime("%Y-%m-%d")
    return dim_date[["date_id", "order_date", "year", "month", "month_name", "quarter"]]


def transform_to_star_schema(df: pd.DataFrame) -> tuple[pd.DataFrame, ...]:
    dim_date = create_dim_date(df)
    dim_region = build_dimension(df["region"], "region_id", "region_name")
    dim_channel = build_dimension(df["channel"], "channel_id", "channel_name")
    dim_segment = build_dimension(df["customer_segment"], "segment_id", "segment_name")
    dim_category = build_dimension(df["category"], "category_id", "category_name")

    category_map = dict(zip(dim_category["category_name"], dim_category["category_id"]))

    dim_product = (
        df[["product_name", "category"]]
        .drop_duplicates()
        .sort_values(["category", "product_name"])
        .reset_index(drop=True)
    )
    dim_product["product_id"] = np.arange(1, len(dim_product) + 1)
    dim_product["category_id"] = dim_product["category"].map(category_map).astype(int)
    dim_product = dim_product[["product_id", "product_name", "category_id"]]

    date_map = dict(zip(dim_date["order_date"], dim_date["date_id"]))
    region_map = dict(zip(dim_region["region_name"], dim_region["region_id"]))
    channel_map = dict(zip(dim_channel["channel_name"], dim_channel["channel_id"]))
    segment_map = dict(zip(dim_segment["segment_name"], dim_segment["segment_id"]))
    product_map = {}

    product_key_df = (
        df[["product_name", "category"]]
        .drop_duplicates()
        .merge(
            dim_product.merge(dim_category, on="category_id", how="left"),
            left_on=["product_name", "category"],
            right_on=["product_name", "category_name"],
            how="left",
        )
    )
    for record in product_key_df[["product_name", "category", "product_id"]].itertuples(index=False):
        product_map[(record.product_name, record.category)] = int(record.product_id)

    fact_sales = df.copy()
    fact_sales["date_id"] = fact_sales["order_date"].map(date_map).astype(int)
    fact_sales["region_id"] = fact_sales["region"].map(region_map).astype(int)
    fact_sales["channel_id"] = fact_sales["channel"].map(channel_map).astype(int)
    fact_sales["segment_id"] = fact_sales["customer_segment"].map(segment_map).astype(int)
    fact_sales["product_id"] = [
        product_map[(name, category)]
        for name, category in zip(fact_sales["product_name"], fact_sales["category"])
    ]

    fact_sales = fact_sales[
        [
            "order_id",
            "date_id",
            "region_id",
            "channel_id",
            "segment_id",
            "product_id",
            "units_sold",
            "unit_price",
            "discount_pct",
            "gross_revenue",
            "net_revenue",
            "cost",
            "profit",
        ]
    ]

    return dim_date, dim_region, dim_channel, dim_segment, dim_category, dim_product, fact_sales


def load_to_sqlite(
    dim_date: pd.DataFrame,
    dim_region: pd.DataFrame,
    dim_channel: pd.DataFrame,
    dim_segment: pd.DataFrame,
    dim_category: pd.DataFrame,
    dim_product: pd.DataFrame,
    fact_sales: pd.DataFrame,
) -> None:
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")

    drop_sql = """
    PRAGMA foreign_keys = OFF;
    DROP TABLE IF EXISTS fact_sales;
    DROP TABLE IF EXISTS dim_product;
    DROP TABLE IF EXISTS dim_category;
    DROP TABLE IF EXISTS dim_customer_segment;
    DROP TABLE IF EXISTS dim_channel;
    DROP TABLE IF EXISTS dim_region;
    DROP TABLE IF EXISTS dim_date;
    PRAGMA foreign_keys = ON;
    """

    with sqlite3.connect(DB_PATH) as connection:
        connection.executescript(drop_sql)
        connection.executescript(schema_sql)

        dim_date.to_sql("dim_date", connection, if_exists="append", index=False)
        dim_region.to_sql("dim_region", connection, if_exists="append", index=False)
        dim_channel.to_sql("dim_channel", connection, if_exists="append", index=False)
        dim_segment.to_sql("dim_customer_segment", connection, if_exists="append", index=False)
        dim_category.to_sql("dim_category", connection, if_exists="append", index=False)
        dim_product.to_sql("dim_product", connection, if_exists="append", index=False)
        fact_sales.to_sql("fact_sales", connection, if_exists="append", index=False)


def main() -> None:
    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"Missing input data: {RAW_FILE}. Run `python src/generate_data.py` first."
        )

    sales_df = pd.read_csv(RAW_FILE)
    transformed = transform_to_star_schema(sales_df)
    load_to_sqlite(*transformed)

    dim_date, dim_region, dim_channel, dim_segment, dim_category, dim_product, fact_sales = transformed
    print(f"Warehouse created: {DB_PATH}")
    print(f"Rows loaded -> fact_sales: {len(fact_sales):,}")
    print(
        "Dimension sizes -> "
        f"dates: {len(dim_date)}, regions: {len(dim_region)}, channels: {len(dim_channel)}, "
        f"segments: {len(dim_segment)}, categories: {len(dim_category)}, products: {len(dim_product)}"
    )


if __name__ == "__main__":
    main()