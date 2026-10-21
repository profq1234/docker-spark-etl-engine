#!/bin/bash

# Exit immediately if any command fails
set -e

echo "=================================================="
echo "🚀 Booting Algorithmic Trading Data Lake Pipeline"
echo "=================================================="

echo "[1/4] Starting Docker Infrastructure (MinIO & Spark)..."
docker compose up -d

# Give MinIO a few seconds to fully initialize before sending data to it
echo "Waiting 10 seconds for MinIO to initialize..."
sleep 10

echo "[2/4] Executing Bronze Layer (API Extraction)..."
python3 src/data_generator.py

echo "📦 Installing PySpark dependencies inside the container..."
docker compose exec spark-master pip3 install python-dotenv

echo "[3/4] Executing Silver Layer (Spark ETL & Cleaning)..."
docker compose exec spark-master /spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026 /opt/workspace/spark_etl_job.py

echo "[4/4] Executing Gold Layer (Spark Aggregation)..."
docker compose exec spark-master /spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026 /opt/workspace/gold_aggregation_job.py

echo "=================================================="
echo "✅ Pipeline Execution Complete!"
echo "Access MinIO Data Lake at http://localhost:9001"
echo "=================================================="
