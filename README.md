# Currency ETL Pipeline (Python, Pandas, SQLite)

## Overview
This project is a simple end-to-end ETL (Extract, Transform, Load) pipeline built using Python.  
The pipeline extracts foreign exchange rate data from a public API, transforms the raw JSON data into a structured tabular format, and loads the processed data into both a CSV file and a local SQLite database.

This project was created as a learning exercise to practice basic data engineering workflows, including data ingestion, transformation, logging, and database loading.

---

## Data Source
- **API**: Frankfurter API  
- **Endpoint**: `https://api.frankfurter.app/latest?from=USD`  
- **Data**: Daily foreign exchange rates with USD as the base currency.

---

## Pipeline Flow
1. **Extract**
   - Fetches currency exchange rate data from a public API using HTTP requests with retry logic.
   - Handles basic network or server errors.

2. **Transform**
   - Converts JSON data into a Pandas DataFrame.
   - Adds metadata such as base currency, date, and load timestamp (UTC).
   - Performs basic data quality checks (e.g. removing missing values).

3. **Load**
   - Writes the transformed data to a CSV file.
   - Loads the data into a SQLite database using an upsert strategy to avoid duplicate records.

---

## Dependencies Used
- Python
- Pandas
- Requests
- SQLite
- Logging

---

## How to Run
```bash
git clone https://github.com/USERNAME/currency-etl-pipeline.git
cd currency-etl-pipeline
pip install -r requirements.txt
python etl_currency_pipeline.py

## Notes
SQLite is used for simplicity and ease of setup.
This pipeline is intended for local execution and learning purposes.
