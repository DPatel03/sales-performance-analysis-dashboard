# Sales Performance Analysis & Dashboard

This project demonstrates an end-to-end analytics workflow using Python, SQL, and interactive reporting.

## Project Goals

- Perform exploratory data analysis (EDA) to identify trends, seasonality, and outliers.
- Query relational data with SQL joins, aggregations, and filters.
- Build a dashboard for stakeholders to explore KPIs and performance drivers.

## Tech Stack

- Python (Pandas, NumPy)
- SQL (SQLite)
- Visualization (Matplotlib, Seaborn)
- Dashboard (Streamlit)

## Repository Structure

```text
sales-performance-analysis-dashboard/
  dashboard/
    app.py
  data/
    raw/
    warehouse/
  outputs/
    charts/
    tables/
  sql/
    analysis_queries.sql
    schema.sql
  src/
    analyze_sales.py
    generate_data.py
    load_data.py
  requirements.txt
```

## How to Run

1. Create and activate a virtual environment.
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Generate synthetic sales transactions:

   ```bash
   python src/generate_data.py
   ```

4. Load data into a relational SQLite warehouse:

   ```bash
   python src/load_data.py
   ```

5. Run analysis and create charts/tables:

   ```bash
   python src/analyze_sales.py
   ```

6. Launch the interactive dashboard:

   ```bash
   streamlit run dashboard/app.py
   ```

## Outputs Produced

- Summary KPI table (revenue, profit, margin, average order value)
- Monthly trend table and line chart
- Region and category performance tables/charts
- Outlier month detection table using z-scores

## Notes

- This project uses generated sample data so it can run immediately without external files.
- If you want to replace with real company data, keep column names and rerun `load_data.py`.
