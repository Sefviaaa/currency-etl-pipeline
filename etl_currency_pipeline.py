import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
import sqlite3
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

API_URL = "https://api.frankfurter.app/latest?from=USD"
CSV_FILEPATH = "exchange_rates.csv"
DB_FILEPATH = "exchange_rates.db"
TABLE_NAME = "exchange_rates"

# Setup requests session with retries
def create_session(retries=3, backoff_factor=0.3, status_forcelist=(500,502,503,504)):
    session = requests.Session()
    retries_cfg = Retry(total=retries, backoff_factor=backoff_factor,
                        status_forcelist=status_forcelist, allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retries_cfg)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

session = create_session()

def extract_data():
    logging.info("Starting data extraction from API.")
    resp = session.get(API_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    logging.info("Data extraction completed.")
    return data

def transform_data(data):
    logging.info("Starting data transformation.")
    rates = data.get("rates", {})
    df = pd.DataFrame(list(rates.items()), columns=["currency", "rate"])
    df["base_currency"] = data.get("base", "USD")
    df["date"] = data.get("date", None)
    # timezone-aware timestamp string
    df["loaded_at"] = datetime.now(timezone.utc).isoformat()
    # basic data quality: drop rows with missing currency or rate
    df = df.dropna(subset=["currency", "rate"])
    df = df.sort_values(by="rate", ascending=False)
    logging.info("Data transformation completed.")
    return df

def load_to_csv(df, filepath=CSV_FILEPATH):
    logging.info(f"Loading data to CSV at {filepath}.")
    df.to_csv(filepath, index=False)
    logging.info("CSV saved successfully.")

def init_sqlite_db(db_path=DB_FILEPATH):
    # Create table with unique constraint to avoid duplicates per currency+date
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        currency TEXT NOT NULL,
        rate REAL,
        base_currency TEXT,
        date TEXT,
        loaded_at TEXT,
        PRIMARY KEY (currency, date)
    );
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute(create_sql)
        conn.commit()

def load_to_sqlite_upsert(df, db_path=DB_FILEPATH):
    logging.info(f"Loading data to SQLite database at {db_path} (upsert).")
    init_sqlite_db(db_path)
    insert_sql = f"""
    INSERT OR REPLACE INTO {TABLE_NAME} (currency, rate, base_currency, date, loaded_at)
    VALUES (?, ?, ?, ?, ?);
    """
    rows = df[["currency","rate","base_currency","date","loaded_at"]].values.tolist()
    with sqlite3.connect(db_path) as conn:
        conn.executemany(insert_sql, rows)
        conn.commit()
    logging.info("Data saved to SQLite database successfully (upsert).")

def run_pipeline():
    logging.info("Running ETL pipeline.")
    try:
        raw = extract_data()
        df = transform_data(raw)
        logging.info("Sample of transformed data:\n%s", df.head().to_string())
        load_to_csv(df)
        load_to_sqlite_upsert(df)
        logging.info("ETL pipeline completed successfully.")
    except Exception as e:
        logging.exception("ETL pipeline failed")  # logs stack trace

if __name__ == "__main__":
    run_pipeline()
