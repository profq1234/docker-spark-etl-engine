#!/bin/bash

# Exit immediately if any command fails
set -e

echo "=================================================="
echo "🚀 Booting Algorithmic Trading Data Lake Pipeline"
echo "=================================================="

echo "[1/4] Starting Docker Infrastructure (MinIO & Spark)..."
docker compose up -d

# Give MinIO a few seconds to fully initialize
echo "Waiting 10 seconds for MinIO to initialize..."
sleep 10

echo "[2/4] Executing Bronze Layer (API Extraction)..."
python3 src/data_generator.py

echo "🪣 Initializing Silver and Gold Zones in MinIO..."
pip install boto3 python-dotenv > /dev/null
python3 -c "
import os, boto3
from dotenv import load_dotenv
load_dotenv('.env')
s3 = boto3.client('s3', endpoint_url='http://localhost:9000', aws_access_key_id=os.getenv('MINIO_ROOT_USER'), aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'))
for bucket in ['silver-zone', 'gold-zone']:
    try:
        s3.create_bucket(Bucket=bucket)
        print(f'✅ Created bucket: {bucket}')
    except Exception as e:
        print(f'⚠️ {bucket} already exists or error: {e}')
"

echo "📦 Installing PySpark dependencies inside the container..."
docker compose exec spark-master pip3 install python-dotenv

echo "🔐 Passing credentials into the Docker container volume..."
cp .env src/.env

echo "[3/4] Executing Silver Layer (Spark ETL & Cleaning)..."
docker compose exec spark-master /spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026 /opt/workspace/spark_etl_job.py

echo "[4/4] Executing Gold Layer (Spark Aggregation)..."
docker compose exec spark-master /spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026 /opt/workspace/gold_aggregation_job.py

echo "=================================================="
echo "✅ Pipeline Execution Complete!"
echo "Access MinIO Data Lake at http://localhost:9001"
echo "=================================================="
