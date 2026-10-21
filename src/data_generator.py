import os
import yfinance as yf
import pandas as pd
import boto3
from botocore.client import Config
from io import StringIO
from dotenv import load_dotenv

# 1. Load credentials from .env
load_dotenv()
MINIO_URL = os.getenv('MINIO_URL', 'http://localhost:9000')
ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'admin')
SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD', 'password123')

# 2. Connect to MinIO
s3 = boto3.resource('s3',
                    endpoint_url=MINIO_URL,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY,
                    config=Config(signature_version='s3v4'))

bucket_name = "bronze-zone"

def setup_bucket():
    """Ensure the bronze-zone bucket exists."""
    if s3.Bucket(bucket_name) not in s3.buckets.all():
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Created MinIO Bucket: {bucket_name}")

def fetch_and_upload_stock_data(ticker_symbol):
    """Fetches real historical data and uploads it to MinIO."""
    print(f"📥 Fetching 5-year historical data for {ticker_symbol}...")
    
    # Fetch 5 years of daily data
    stock_data = yf.download(ticker_symbol, period="5y", interval="1d")
    
    # Check if we got data
    if stock_data.empty:
        print(f"❌ Failed to fetch data for {ticker_symbol}")
        return

    # Add the ticker symbol as a column so we know which stock is which
    stock_data['Ticker'] = ticker_symbol
    
    # Convert Pandas DataFrame directly to a CSV string in memory
    # We do this so we don't have to save files to your local hard drive first
    csv_buffer = StringIO()
    stock_data.to_csv(csv_buffer)
    
    # The file path inside MinIO
    object_name = f"raw_stocks/{ticker_symbol}_5yr.csv"
    
    # Upload the in-memory string directly to MinIO
    print(f"☁️ Uploading {ticker_symbol} to MinIO ({object_name})...")
    s3.Object(bucket_name, object_name).put(Body=csv_buffer.getvalue())
    print(f"✅ Upload Complete for {ticker_symbol}!")

def main():
    setup_bucket()
    
    # List of stocks to track
    portfolio = ['AAPL', 'MSFT', 'NVDA', 'TSLA']
    
    for stock in portfolio:
        fetch_and_upload_stock_data(stock)
        
    print("\n🎉 All real market data is now sitting in your MinIO Data Lake!")

if __name__ == "__main__":
    main()
