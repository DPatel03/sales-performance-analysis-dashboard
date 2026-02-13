import sqlite3
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


matplotlib.use("Agg")

ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "data" / "warehouse" / "sales.db"
TABLE_OUTPUT_DIR = ROOT_DIR / "outputs" / "tables"
CHART_OUTPUT_DIR = ROOT_DIR / "outputs" / "charts"


KPI_QUERY = """
SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(net_revenue), 2) AS total_revenue,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(AVG(net_revenue), 2) AS avg_order_value,
    ROUND((SUM(profit) / NULLIF(SUM(net_revenue), 0)) * 100, 2) AS profit_margin_pct
FROM fact_sales;
"""

MONTHLY_QUERY = """
SELECT
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(f.net_revenue), 2) AS net_revenue,
    ROUND(SUM(f.profit), 2) AS profit,
    ROUND((SUM(f.profit) / NULLIF(SUM(f.net_revenue), 0)) * 100, 2) AS margin_pct,
    COUNT(*) AS orders
FROM fact_sales f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
"""

REGION_QUERY = """
SELECT
    r.region_name AS region,
    ROUND(SUM(f.net_revenue), 2) AS revenue,
    ROUND(SUM(f.profit), 2) AS profit,
    ROUND((SUM(f.profit) / NULLIF(SUM(f.net_revenue), 0)) * 100, 2) AS margin_pct
FROM fact_sales f
JOIN dim_region r ON r.region_id = f.region_id
GROUP BY r.region_name
ORDER BY revenue DESC;
"""

CATEGORY_QUERY = """
SELECT
    c.category_name AS category,
    ROUND(SUM(f.net_revenue), 2) AS revenue,
    ROUND(SUM(f.profit), 2) AS profit,
    ROUND(SUM(f.units_sold), 0) AS units_sold
FROM fact_sales f
JOIN dim_product p ON p.product_id = f.product_id
JOIN dim_category c ON c.category_id = p.category_id
GROUP BY c.category_name
ORDER BY revenue DESC;
"""

SEASONALITY_QUERY = """
SELECT
    d.month,
    d.month_name,
    ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue
FROM (
    SELECT
        f.date_id,
        SUM(f.net_revenue) AS daily_revenue
    FROM fact_sales f
    GROUP BY f.date_id
) daily
JOIN dim_date d ON d.date_id = daily.date_id
GROUP BY d.month, d.month_name
ORDER BY d.month;
"""


def detect_monthly_outliers(monthly_df: pd.DataFrame, threshold: float = 2.0) -> pd.DataFrame:
    outlier_df = monthly_df.copy()
    mean_value = outlier_df["net_revenue"].mean()
    std_value = outlier_df["net_revenue"].std(ddof=0)
    if std_value == 0:
        outlier_df["z_score"] = 0.0
    else:
        outlier_df["z_score"] = (outlier_df["net_revenue"] - mean_value) / std_value

    return outlier_df.loc[outlier_df["z_score"].abs() >= threshold].sort_values(
        "z_score", ascending=False
    )


def save_tables_and_charts(
    kpi_df: pd.DataFrame,
    monthly_df: pd.DataFrame,
    region_df: pd.DataFrame,
    category_df: pd.DataFrame,
    seasonality_df: pd.DataFrame,
    outlier_df: pd.DataFrame,
) -> None:
    TABLE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CHART_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    kpi_df.to_csv(TABLE_OUTPUT_DIR / "kpi_summary.csv", index=False)
    monthly_df.to_csv(TABLE_OUTPUT_DIR / "monthly_performance.csv", index=False)
    region_df.to_csv(TABLE_OUTPUT_DIR / "region_performance.csv", index=False)
    category_df.to_csv(TABLE_OUTPUT_DIR / "category_performance.csv", index=False)
    seasonality_df.to_csv(TABLE_OUTPUT_DIR / "seasonality_profile.csv", index=False)
    outlier_df.to_csv(TABLE_OUTPUT_DIR / "monthly_outliers.csv", index=False)

    sns.set_theme(style="whitegrid")

    monthly_chart_df = monthly_df.copy()
    monthly_chart_df["period"] = pd.to_datetime(
        monthly_chart_df["year"].astype(str)
        + "-"
        + monthly_chart_df["month"].astype(str).str.zfill(2)
        + "-01"
    )
    plt.figure(figsize=(12, 5))
    sns.lineplot(data=monthly_chart_df, x="period", y="net_revenue", marker="o")
    plt.title("Monthly Net Revenue Trend")
    plt.xlabel("Month")
    plt.ylabel("Net Revenue")
    plt.tight_layout()
    plt.savefig(CHART_OUTPUT_DIR / "monthly_net_revenue_trend.png", dpi=200)
    plt.close()

    plt.figure(figsize=(10, 5))
    sns.barplot(
        data=region_df,
        x="region",
        y="margin_pct",
        hue="region",
        palette="Blues_d",
        legend=False,
    )
    plt.title("Profit Margin by Region")
    plt.xlabel("Region")
    plt.ylabel("Margin (%)")
    plt.tight_layout()
    plt.savefig(CHART_OUTPUT_DIR / "profit_margin_by_region.png", dpi=200)
    plt.close()

    plt.figure(figsize=(10, 5))
    sns.barplot(
        data=category_df,
        x="category",
        y="revenue",
        hue="category",
        palette="viridis",
        legend=False,
    )
    plt.title("Revenue by Category")
    plt.xlabel("Category")
    plt.ylabel("Revenue")
    plt.xticks(rotation=15)
    plt.tight_layout()
    plt.savefig(CHART_OUTPUT_DIR / "revenue_by_category.png", dpi=200)
    plt.close()

    plt.figure(figsize=(10, 5))
    sns.lineplot(data=seasonality_df, x="month_name", y="avg_daily_revenue", marker="o")
    plt.title("Seasonality: Average Daily Revenue by Month")
    plt.xlabel("Month")
    plt.ylabel("Average Daily Revenue")
    plt.tight_layout()
    plt.savefig(CHART_OUTPUT_DIR / "seasonality_avg_daily_revenue.png", dpi=200)
    plt.close()


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Missing warehouse at {DB_PATH}. Run `python src/load_data.py` first."
        )

    with sqlite3.connect(DB_PATH) as connection:
        kpi_df = pd.read_sql_query(KPI_QUERY, connection)
        monthly_df = pd.read_sql_query(MONTHLY_QUERY, connection)
        region_df = pd.read_sql_query(REGION_QUERY, connection)
        category_df = pd.read_sql_query(CATEGORY_QUERY, connection)
        seasonality_df = pd.read_sql_query(SEASONALITY_QUERY, connection)

    outlier_df = detect_monthly_outliers(monthly_df)
    save_tables_and_charts(
        kpi_df=kpi_df,
        monthly_df=monthly_df,
        region_df=region_df,
        category_df=category_df,
        seasonality_df=seasonality_df,
        outlier_df=outlier_df,
    )

    print("Analysis complete.")
    print(f"KPI summary saved to: {TABLE_OUTPUT_DIR / 'kpi_summary.csv'}")
    print(f"Charts saved to: {CHART_OUTPUT_DIR}")
    if outlier_df.empty:
        print("No monthly outliers detected at z-score threshold >= 2.0")
    else:
        print(f"Outlier months detected: {len(outlier_df)}")


if __name__ == "__main__":
    main()