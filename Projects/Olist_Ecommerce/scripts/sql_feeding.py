"""
sql_feeding.py
--------------
Loads olist_order_payments_dataset.csv  →  MySQL
Loads product_category_name_translation.csv  →  MongoDB

Run locally:    python scripts/sql_feeding.py
Run in Docker:  docker run --env-file .env olist-feeding
"""

import os
import sys
import pandas as pd
import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient
from dotenv import load_dotenv

# ── Load credentials from .env (ignored in Docker when --env-file is used) ──
load_dotenv()

# ── MySQL config ─────────────────────────────────────────────────────────────
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB   = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASS")

# ── MongoDB config ────────────────────────────────────────────────────────────
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DB   = os.getenv("MONGO_DB")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")

# ── CSV paths ─────────────────────────────────────────────────────────────────
ORDER_PAYMENTS_CSV       = os.getenv("ORDER_PAYMENTS_CSV",       "data/olist_order_payments_dataset.csv")
CATEGORY_TRANSLATION_CSV = os.getenv("CATEGORY_TRANSLATION_CSV", "data/product_category_name_translation.csv")


# ─────────────────────────────────────────────────────────────────────────────
def validate_env():
    """Fail fast if any required variable is missing."""
    required = {
        "MYSQL_HOST": MYSQL_HOST, "MYSQL_DB": MYSQL_DB,
        "MYSQL_USER": MYSQL_USER, "MYSQL_PASS": MYSQL_PASS,
        "MONGO_HOST": MONGO_HOST, "MONGO_DB":   MONGO_DB,
        "MONGO_USER": MONGO_USER, "MONGO_PASS": MONGO_PASS,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"[ERROR] Missing environment variables: {', '.join(missing)}")
        print("        Copy .env.example → .env and fill in your credentials.")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
def feed_mysql():
    """Load olist_order_payments_dataset.csv into MySQL."""
    table_name = "olist_order_payments"
    connection = None

    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            port=int(MYSQL_PORT),
        )

        if not connection.is_connected():
            raise RuntimeError("Connection object created but not connected.")

        print(f"[MySQL] Connected — server version: {connection.get_server_info()}")
        cursor = connection.cursor()

        # Drop + recreate for idempotent runs
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                order_id              VARCHAR(50),
                payment_sequential    INT,
                payment_type          VARCHAR(20),
                payment_installments  INT,
                payment_value         FLOAT
            )
        """)
        print(f"[MySQL] Table '{table_name}' created")

        data          = pd.read_csv(ORDER_PAYMENTS_CSV)
        total_records = len(data)
        batch_size    = 1000

        insert_query = f"""
            INSERT INTO {table_name}
                (order_id, payment_sequential, payment_type, payment_installments, payment_value)
            VALUES (%s, %s, %s, %s, %s)
        """

        for start in range(0, total_records, batch_size):
            batch   = data.iloc[start : start + batch_size]
            records = [tuple(row) for row in batch.itertuples(index=False, name=None)]
            cursor.executemany(insert_query, records)
            connection.commit()
            end = min(start + batch_size, total_records)
            print(f"[MySQL] Inserted rows {start + 1}–{end}")

        print(f"[MySQL] Done — {total_records} rows in '{table_name}'")

    except Error as e:
        print(f"[MySQL] Error: {e}")
        sys.exit(1)

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("[MySQL] Connection closed")


# ─────────────────────────────────────────────────────────────────────────────
def feed_mongodb():
    """Load product_category_name_translation.csv into MongoDB."""
    uri    = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}"
    client = None

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # Force connection check
        client.server_info()
        db = client[MONGO_DB]

        print(f"[MongoDB] Connected — collections: {db.list_collection_names()}")

        df      = pd.read_csv(CATEGORY_TRANSLATION_CSV)
        records = df.to_dict(orient="records")

        collection = db["product_category_translation"]
        collection.drop()   # idempotent — clean slate each run
        collection.insert_many(records)

        print(f"[MongoDB] Inserted {len(records)} documents")
        print(f"[MongoDB] Sample: {collection.find_one(projection={'_id': 0})}")

    except Exception as e:
        print(f"[MongoDB] Error: {e}")
        sys.exit(1)

    finally:
        if client:
            client.close()
            print("[MongoDB] Connection closed")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    validate_env()

    print("\n=== Step 1 / 2 — Feeding MySQL ===")
    feed_mysql()

    print("\n=== Step 2 / 2 — Feeding MongoDB ===")
    feed_mongodb()

    print("\n✅  All feeds completed successfully.")
