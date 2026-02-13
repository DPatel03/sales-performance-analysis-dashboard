-- KPI summary
SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(net_revenue), 2) AS total_revenue,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(AVG(net_revenue), 2) AS avg_order_value,
    ROUND((SUM(profit) / NULLIF(SUM(net_revenue), 0)) * 100, 2) AS profit_margin_pct
FROM fact_sales;

-- Monthly trend
SELECT
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(f.net_revenue), 2) AS net_revenue,
    ROUND(SUM(f.profit), 2) AS profit,
    COUNT(*) AS orders
FROM fact_sales f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- Region performance
SELECT
    r.region_name AS region,
    ROUND(SUM(f.net_revenue), 2) AS revenue,
    ROUND(SUM(f.profit), 2) AS profit,
    ROUND((SUM(f.profit) / NULLIF(SUM(f.net_revenue), 0)) * 100, 2) AS margin_pct
FROM fact_sales f
JOIN dim_region r ON r.region_id = f.region_id
GROUP BY r.region_name
ORDER BY revenue DESC;

-- Category performance
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