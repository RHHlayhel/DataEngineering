# Olist Brazilian E-Commerce — Data Engineering Project

**Data source:** [Kaggle — Brazilian E-Commerce (Olist)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

The data is cut into 2 sets: 
The 7 Bronze CSVs are sourced via Azure Data Factory from: [https://github.com/BigDataProjects/...](https://github.com/mayank953/BigDataProjects/tree/main/Project-Brazillian%20Ecommerce)
The 2 additional CSVs (order payments, category translation) must be downloaded manually from Kaggle: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Architecture

```
Kaggle CSVs
    │
    ▼
Azure Data Factory (ForEachInput.json)
    │  copies 7 CSVs via HTTP
    ▼
ADLS Gen2 — Bronze/
    │
    ├──── Docker container (sql_feeding.py)
    │         olist_order_payments  ──► MySQL  (Filess.io)
    │         product_category_translation ──► MongoDB (Filess.io)
    │
    ▼
Databricks — 02_ecommerce_silver.ipynb
    │  reads Bronze CSVs + MongoDB
    │  cleans, casts dates, computes delivery KPIs
    │  joins all 9 datasets
    ▼
ADLS Gen2 — Silver/full_orders/   (Delta)
    │
    ▼
Databricks — 03_gold_layer.ipynb
    │  aggregations → 5 business-ready tables
    ▼
ADLS Gen2 — Gold/
    │
    ▼
Power BI (connect directly to Gold Delta tables)
```

---

## Project Structure

```
olist-project/
├── notebooks/
│   ├── 01_SQLFeeding.ipynb          # MySQL + MongoDB ingestion (Databricks/Colab)
│   ├── 02_ecommerce_silver.ipynb    # Bronze → Silver transformation (Databricks)
│   └── 03_gold_layer.ipynb          # Silver → Gold aggregations (Databricks)
├── scripts/
│   └── sql_feeding.py               # Production version of notebook 01 (runs in Docker)
├── pipeline/
│   └── ForEachInput.json            # ADF ForEach activity parameters
├── data/                            # Local CSVs (gitignored — download from Kaggle)
│   ├── olist_order_payments_dataset.csv
│   └── product_category_name_translation.csv
├── .env.example                     # Credential template — copy to .env
├── .gitignore
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Clone & configure credentials

```bash
git clone https://github.com/RHHlayhel/DataEngineering.git
cd DataEngineering/Projects/Olist_Ecommerce
cp .env.example .env
# Edit .env with your real MySQL and MongoDB credentials
```

### 2. Download the two extra CSVs from Kaggle

Place these in `data/`:
- `olist_order_payments_dataset.csv`
- `product_category_name_translation.csv`

### 3. Run the feeding script (Docker)

```bash
# Build
docker build -t olist-feeding .

# Run — credentials injected at runtime, never baked into the image
docker run --env-file .env olist-feeding
```

### 4. Run on Databricks

Upload notebooks `01`, `02`, `03` to your Databricks workspace and run them in order.

For credentials on Databricks, use **Databricks Secrets** instead of `.env`:

```bash
# One-time setup via CLI
databricks secrets create-scope --scope olist
databricks secrets put --scope olist --key mongo_host
databricks secrets put --scope olist --key mongo_pass
# ... etc.
```

Then in the notebook:
```python
MONGO_HOST = dbutils.secrets.get(scope='olist', key='mongo_host')
MONGO_PASS = dbutils.secrets.get(scope='olist', key='mongo_pass')
```

---

## Gold Tables Produced

| Table                       | Description                                                 |
| --------------------------- | ----------------------------------------------------------- |
| `revenue_by_category`       | Total revenue, order count, avg value per product category  |
| `delivery_performance`      | Avg delivery days and delay rate per seller state           |
| `monthly_revenue_trend`     | Revenue and order volume by month                           |
| `review_score_distribution` | Review scores correlated with delivery time and order value |
| `top_sellers`               | Top 100 sellers by revenue with avg review score            |

---

## Dependencies

| Package | Purpose |
|---|---|
| `pandas` | DataFrame operations in feeding scripts |
| `mysql-connector-python` | MySQL ingestion |
| `pymongo` | MongoDB ingestion |
| `python-dotenv` | Load `.env` credentials locally |
| `pyspark` | Provided by Databricks cluster — not in Docker image |
