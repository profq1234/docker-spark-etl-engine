from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, round

MINIO_URL = "http://minio:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "password123"

def main():
    print("🚀 Initializing Spark Session for Gold Layer...")
    
    spark = SparkSession.builder \
        .appName("Silver_to_Gold_ETL") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
        
    print("📥 Reading cleaned Parquet data from Silver Zone...")
    silver_df = spark.read.parquet("s3a://silver-zone/cleaned_stocks/")
    
    print("⚙️ Calculating Trading Metrics...")
    # Explicitly cast string columns to 'double' (numeric) before calculating metrics
    gold_df = silver_df.groupBy("Ticker").agg(
        round(avg("Daily_Spread"), 2).alias("Avg_Daily_Volatility"),
        max(col("High").cast("double")).alias("5_Year_High"),
        round(avg(col("Volume").cast("double")), 0).alias("Avg_Daily_Volume")
    )
    
    gold_df.show()
    
    print("☁️ Writing business-ready data to Gold Zone...")
    gold_df.write \
        .mode("overwrite") \
        .parquet("s3a://gold-zone/trading_metrics/")
        
    print("✅ Gold ETL Job Complete! The data lake pipeline is fully built.")
    spark.stop()

if __name__ == "__main__":
    main()
