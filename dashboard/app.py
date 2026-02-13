import sys
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st


ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "data" / "warehouse" / "sales.db"

# Ensure project root is on path so we can run data pipeline when deployed
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

BASE_QUERY = """
SELECT
    f.order_id,
    d.order_date,
    d.year,
    d.month,
    r.region_name AS region,
    ch.channel_name AS channel,
    seg.segment_name AS customer_segment,
    c.category_name AS category,
    p.product_name,
    f.units_sold,
    f.unit_price,
    f.discount_pct,
    f.net_revenue,
    f.profit
FROM fact_sales f
JOIN dim_date d ON d.date_id = f.date_id
JOIN dim_region r ON r.region_id = f.region_id
JOIN dim_channel ch ON ch.channel_id = f.channel_id
JOIN dim_customer_segment seg ON seg.segment_id = f.segment_id
JOIN dim_product p ON p.product_id = f.product_id
JOIN dim_category c ON c.category_id = p.category_id;
"""


@st.cache_data(show_spinner=False)
def load_sales_data(database_path: Path) -> pd.DataFrame:
    with sqlite3.connect(database_path) as connection:
        df = pd.read_sql_query(BASE_QUERY, connection, parse_dates=["order_date"])
    return df


def detect_outlier_months(monthly_df: pd.DataFrame, threshold: float = 2.0) -> pd.DataFrame:
    result = monthly_df.copy()
    std_value = result["net_revenue"].std(ddof=0)
    if std_value == 0:
        result["z_score"] = 0.0
    else:
        result["z_score"] = (result["net_revenue"] - result["net_revenue"].mean()) / std_value
    return result.loc[result["z_score"].abs() >= threshold].sort_values("z_score", ascending=False)


def main() -> None:
    st.set_page_config(page_title="Sales Dashboard", page_icon="📊", layout="wide")
    st.markdown(
        """
        <style>
        [data-testid="stMetric"] {
            background-color: rgba(255, 255, 255, 0.05);
            padding: 0.75rem 1rem;
            border-radius: 8px;
            border: 1px solid rgba(255, 255, 255, 0.12);
        }
        [data-testid="stMetric"] label {
            font-size: 0.85rem;
        }
        .kpi-spacer {
            margin-bottom: 2rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("Sales Performance Analysis & Dashboard")
    st.caption("SQL + Python analytics project with interactive filtering and KPI tracking.")

    if not DB_PATH.exists():
        st.info("Building database for the first time (this may take a moment)...")
        try:
            from src.generate_data import main as run_generate_data
            from src.load_data import main as run_load_data

            with st.spinner("Generating sample data and loading into warehouse..."):
                run_generate_data()
                run_load_data()
            st.success("Database ready. Reloading...")
            st.rerun()
        except Exception as e:
            st.error("Could not build database automatically.")
            st.code(
                "python src/generate_data.py\npython src/load_data.py\npython src/analyze_sales.py",
                language="bash",
            )
            st.exception(e)
            st.stop()

    sales_df = load_sales_data(DB_PATH)
    if sales_df.empty:
        st.warning("No sales records available.")
        st.stop()

    min_date = sales_df["order_date"].min().date()
    max_date = sales_df["order_date"].max().date()

    st.sidebar.header("Filters")
    date_range = st.sidebar.date_input(
        "Order date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if len(date_range) != 2:
        st.info("Select both start and end dates.")
        st.stop()

    regions = sorted(sales_df["region"].unique().tolist())
    channels = sorted(sales_df["channel"].unique().tolist())
    categories = sorted(sales_df["category"].unique().tolist())
    segments = sorted(sales_df["customer_segment"].unique().tolist())

    selected_regions = st.sidebar.multiselect("Region", options=regions, default=regions)
    selected_channels = st.sidebar.multiselect("Channel", options=channels, default=channels)
    selected_categories = st.sidebar.multiselect("Category", options=categories, default=categories)
    selected_segments = st.sidebar.multiselect("Customer Segment", options=segments, default=segments)

    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    filtered_df = sales_df.loc[
        (sales_df["order_date"] >= start_date)
        & (sales_df["order_date"] <= end_date)
        & (sales_df["region"].isin(selected_regions))
        & (sales_df["channel"].isin(selected_channels))
        & (sales_df["category"].isin(selected_categories))
        & (sales_df["customer_segment"].isin(selected_segments))
    ].copy()

    if filtered_df.empty:
        st.warning(
            "No data for this selection. Try widening the date range or adding more regions, "
            "channels, or categories."
        )
        st.stop()

    total_revenue = float(filtered_df["net_revenue"].sum())
    total_profit = float(filtered_df["profit"].sum())
    total_orders = int(filtered_df["order_id"].nunique())
    avg_order_value = total_revenue / total_orders if total_orders else 0.0
    margin_pct = (total_profit / total_revenue * 100.0) if total_revenue else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Revenue", f"${total_revenue:,.0f}")
    c2.metric("Profit", f"${total_profit:,.0f}")
    c3.metric("Orders", f"{total_orders:,}")
    c4.metric("Avg Order Value", f"${avg_order_value:,.2f}")
    c5.metric("Profit Margin", f"{margin_pct:.2f}%")

    st.markdown('<div class="kpi-spacer"></div>', unsafe_allow_html=True)

    monthly_df = (
        filtered_df.groupby(pd.Grouper(key="order_date", freq="MS"))
        .agg(net_revenue=("net_revenue", "sum"), profit=("profit", "sum"), orders=("order_id", "count"))
        .reset_index()
        .sort_values("order_date")
    )

    region_df = (
        filtered_df.groupby("region", as_index=False)
        .agg(revenue=("net_revenue", "sum"), profit=("profit", "sum"))
        .sort_values("revenue", ascending=False)
    )

    category_df = (
        filtered_df.groupby("category", as_index=False)
        .agg(revenue=("net_revenue", "sum"), units_sold=("units_sold", "sum"))
        .sort_values("revenue", ascending=False)
    )

    st.subheader("Monthly Trend")
    monthly_chart_df = (
        monthly_df.set_index("order_date")[["net_revenue", "profit"]]
        .rename(columns={"net_revenue": "Net Revenue", "profit": "Profit"})
    )
    st.line_chart(monthly_chart_df)

    left, right = st.columns(2)
    with left:
        st.subheader("Revenue by Region")
        st.bar_chart(region_df.set_index("region")["revenue"])
    with right:
        st.subheader("Revenue by Category")
        st.bar_chart(category_df.set_index("category")["revenue"])

    st.subheader("Potential Outlier Months (z-score >= 2)")
    outliers = detect_outlier_months(monthly_df[["order_date", "net_revenue"]])
    if outliers.empty:
        st.write("No outlier months detected for the current filter selection.")
    else:
        formatted = outliers.copy()
        formatted["order_date"] = formatted["order_date"].dt.strftime("%Y-%m")
        st.dataframe(formatted.rename(columns={"order_date": "month"}), use_container_width=True)

    st.subheader("SQL Join Used in the Dashboard")
    st.code(BASE_QUERY.strip(), language="sql")

    st.subheader("Filtered Data Preview")
    preview_cols = [
        "order_id",
        "order_date",
        "region",
        "channel",
        "customer_segment",
        "category",
        "product_name",
        "units_sold",
        "net_revenue",
        "profit",
    ]
    st.dataframe(filtered_df[preview_cols].head(200), use_container_width=True)


if __name__ == "__main__":
    main()