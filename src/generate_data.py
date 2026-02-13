from pathlib import Path

import numpy as np
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = ROOT_DIR / "data" / "raw"
OUTPUT_FILE = RAW_DATA_DIR / "sales_transactions.csv"

REGIONS = ["Northeast", "Midwest", "South", "West"]
CHANNELS = ["Online", "Retail", "Wholesale"]
SEGMENTS = ["Consumer", "Corporate", "Home Office", "Small Business"]

CATEGORY_CONFIG = {
    "Electronics": {
        "products": ["Laptop", "Smartphone", "Tablet", "Headphones"],
        "base_price": 620.0,
        "cost_ratio": 0.66,
    },
    "Furniture": {
        "products": ["Desk", "Office Chair", "Bookshelf", "Cabinet"],
        "base_price": 340.0,
        "cost_ratio": 0.61,
    },
    "Office Supplies": {
        "products": ["Printer Paper", "Pen Pack", "Stapler", "Ink Cartridge"],
        "base_price": 44.0,
        "cost_ratio": 0.52,
    },
    "Apparel": {
        "products": ["T-Shirt", "Jacket", "Sneakers", "Backpack"],
        "base_price": 78.0,
        "cost_ratio": 0.57,
    },
    "Food & Beverage": {
        "products": ["Coffee Beans", "Energy Drink", "Snack Box", "Protein Bar"],
        "base_price": 26.0,
        "cost_ratio": 0.60,
    },
}

SEASONALITY = {
    1: 0.92,
    2: 0.95,
    3: 1.02,
    4: 1.03,
    5: 1.05,
    6: 1.09,
    7: 1.12,
    8: 1.07,
    9: 1.00,
    10: 1.04,
    11: 1.24,
    12: 1.38,
}

CHANNEL_DISCOUNT_BOUNDS = {
    "Online": (0.05, 0.18),
    "Retail": (0.01, 0.12),
    "Wholesale": (0.10, 0.28),
}


def generate_sales_transactions(
    start_date: str = "2023-01-01",
    end_date: str = "2025-12-31",
    seed: int = 42,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    categories = list(CATEGORY_CONFIG.keys())
    order_id = 100000
    rows = []

    for current_date in dates:
        daily_orders = max(8, int(rng.poisson(30 * SEASONALITY[current_date.month])))

        for _ in range(daily_orders):
            region = str(rng.choice(REGIONS, p=[0.24, 0.22, 0.31, 0.23]))
            channel = str(rng.choice(CHANNELS, p=[0.56, 0.30, 0.14]))
            segment = str(rng.choice(SEGMENTS, p=[0.52, 0.21, 0.14, 0.13]))
            category = str(rng.choice(categories, p=[0.31, 0.20, 0.24, 0.17, 0.08]))

            config = CATEGORY_CONFIG[category]
            product_name = str(rng.choice(config["products"]))

            units_sold = int(rng.integers(1, 11))
            if rng.random() < 0.008:
                units_sold *= int(rng.integers(6, 15))

            base_price = float(config["base_price"])
            unit_price = max(3.0, round(float(rng.normal(base_price, base_price * 0.14)), 2))

            discount_low, discount_high = CHANNEL_DISCOUNT_BOUNDS[channel]
            discount_pct = round(float(rng.uniform(discount_low, discount_high)), 4)
            if current_date.month in (11, 12) and rng.random() < 0.18:
                discount_pct = min(0.40, round(discount_pct + float(rng.uniform(0.02, 0.08)), 4))

            gross_revenue = round(units_sold * unit_price, 2)
            net_revenue = round(gross_revenue * (1 - discount_pct), 2)

            cost_ratio = float(rng.normal(config["cost_ratio"], 0.04))
            cost_ratio = min(max(cost_ratio, 0.35), 0.90)
            cost = round(gross_revenue * cost_ratio, 2)
            profit = round(net_revenue - cost, 2)

            rows.append(
                {
                    "order_id": order_id,
                    "order_date": current_date.strftime("%Y-%m-%d"),
                    "region": region,
                    "channel": channel,
                    "customer_segment": segment,
                    "category": category,
                    "product_name": product_name,
                    "units_sold": units_sold,
                    "unit_price": unit_price,
                    "discount_pct": discount_pct,
                    "gross_revenue": gross_revenue,
                    "net_revenue": net_revenue,
                    "cost": cost,
                    "profit": profit,
                }
            )
            order_id += 1

    return pd.DataFrame(rows)


def main() -> None:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    sales_df = generate_sales_transactions()
    sales_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Wrote {len(sales_df):,} rows to {OUTPUT_FILE}")
    print(
        "Date coverage:",
        sales_df["order_date"].min(),
        "to",
        sales_df["order_date"].max(),
    )
    print("Sample columns:", ", ".join(sales_df.columns))


if __name__ == "__main__":
    main()