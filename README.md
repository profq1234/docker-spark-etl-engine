# 🐋 Docker Spark ETL Engine: Medallion Architecture

A production-grade, containerized Data Engineering pipeline designed to ingest, process, and aggregate 5-year historical market data for algorithmic trading simulations. Built entirely on local infrastructure using **Apache Spark**, **MinIO (S3-compatible storage)**, and **Docker**.

This project implements a strict **Medallion Architecture (Bronze, Silver, Gold)** to guarantee data quality and mathematical integrity for downstream quantitative backtesting.

## 🏗️ Tech Stack
* **Distributed Compute:** Apache Spark (PySpark)
* **Object Storage:** MinIO (Local S3)
* **Infrastructure & Networking:** Docker, Docker Compose
* **Orchestration:** Bash Scripting, Local CI/CD Principles
* **Data Formats:** CSV (Raw), Snappy-Compressed Parquet (Processed)
* **Languages:** Python 3.10, SQL

---

## ⚙️ The Medallion Pipeline

### 🥉 Bronze Layer (Raw Extraction)
* **Process:** Connects to the `yfinance` API to extract raw, 5-year historical pricing data for a target portfolio (`AAPL`, `MSFT`, `NVDA`, `TSLA`).
* **Storage:** Data is streamed directly into the MinIO `bronze-zone` bucket as raw CSV files. 
* **Goal:** Create an immutable, historical landing zone that perfectly mirrors the source system.

### 🥈 Silver Layer (Cleaning & Compression)
* **Process:** A distributed PySpark job reads the Bronze CSVs across the Docker network. It drops null values, standardizes column names, enforces strict data typing, and calculates daily price spreads.
* **Storage:** Writes to the `silver-zone` bucket partitioned by Ticker, utilizing columnar, Snappy-compressed **Parquet** format to optimize downstream read latency.

### 🥇 Gold Layer (Business Aggregations)
* **Process:** A secondary PySpark job consumes the Silver Parquet data to calculate quantitative trading metrics, including `Avg_Daily_Volatility`, `5_Year_High`, and total trading volume.
* **Storage:** Staged in the `gold-zone` bucket, serving as the pristine, analytical database queried directly by the local Algorithmic Trading simulation.

---

## 🛠️ Engineering Challenges & Solutions

Building this pipeline required solving several silent data corruption and distributed networking bugs:

### 1. The Multi-Index API Drift
* **The Problem:** The source API silently shifted its schema, returning a multi-index header (e.g., the Ticker symbol repeating underneath every standard column header). This caused PySpark's `inferSchema` to break, as it could no longer interpret the headers cleanly.
* **The Solution:** I engineered a data-flattening transformation in the Python extraction layer. By intercepting the Pandas DataFrame before MinIO ingestion and collapsing the multi-index tuples into standard string headers, I ensured the Bronze layer received a normalized schema.

### 2. Alphabetical vs. Numerical Aggregation Anomaly
* **The Problem:** Because the corrupted source headers initially forced PySpark to read numeric columns as strings, the Gold layer's `max()` function began sorting alphabetically rather than mathematically (evaluating `"99.87" > "140.00"`). This resulted in dangerously inaccurate "5-Year High" metrics for the trading bot.
* **The Solution:** I implemented strict schema enforcement in the Silver layer. By explicitly casting columns using `.cast("double")` during the PySpark transformation, I guaranteed the mathematical integrity of all downstream vector aggregations.

### 3. Local S3/Spark Networking Isolation
* **The Problem:** Running Apache Spark and MinIO in decoupled, isolated Docker containers prevented Spark from natively resolving the `s3a://` protocol to read the buckets.
* **The Solution:** I dynamically injected AWS Hadoop dependencies (`hadoop-aws` and `aws-java-sdk-bundle`) via the `--packages` flag at runtime, and configured the `spark-defaults.conf` to route S3 traffic across the internal Docker bridge network (`http://minio:9000`).

---

## 📂 Project Structure
```text
├── src/
│   ├── data_generator.py        # yfinance API extraction script
│   ├── spark_etl_job.py         # Bronze -> Silver processing
│   └── gold_aggregation_job.py  # Silver -> Gold aggregations
├── .env.example                 # Template for MinIO secrets
├── docker-compose.yml           # Infrastructure as Code (Spark + MinIO)
├── requirements.txt             # Python dependencies
├── run_pipeline.sh              # Master orchestration script
└── README.md                    # Project documentation


🚀 Quickstart & Reproducibility
1. Clone the repository and set up the environment:
git clone [https://github.com/profq1234/docker-spark-etl-engine.git](https://github.com/profq1234/docker-spark-etl-engine.git)
cd docker-spark-etl-engine
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

2. Configure Secrets:
cp .env.example .env
# Edit the .env file with your desired MinIO credentials

3. Boot the Infrastructure & Execute the Pipeline:
# Ensure the script is executable
chmod +x run_pipeline.sh

# This spins up the Docker containers and runs the Bronze, Silver, and Gold scripts sequentially
./run_pipeline.sh

4. Verify the Data:
Navigate to http://localhost:9001 in your browser. Log in with your .env credentials to view the Bronze, Silver, and Gold buckets natively in the MinIO UI.
