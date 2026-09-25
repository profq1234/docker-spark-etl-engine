from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# Using internal Docker network addresses - no localhost here!
MINIO_URL = "http://minio:9000" 
ACCESS_KEY = "admin"
SECRET_KEY = "password123"

def main():
    print("🚀 Initializing Spark Session...")
    
    spark = SparkSession.builder \
        .appName("Bronze_to_Silver_ETL") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
        
    print("📥 Reading raw CSV data from Bronze Zone...")
    # Spark pulls all CSVs from the bucket directly into a distributed DataFrame
    bronze_df = spark.read.csv("s3a://bronze-zone/raw_stocks/*.csv", header=True, inferSchema=True)
    
    print("⚙️ Transforming Data...")
    # Clean up column names and calculate the daily price spread
    silver_df = bronze_df \
        .withColumnRenamed("Adj Close", "Adj_Close") \
        .withColumn("Daily_Spread", round(col("High") - col("Low"), 2))
        
    silver_df = silver_df.dropna()
    
    print("☁️ Writing optimized Parquet data to Silver Zone...")
    # Write back to MinIO as Parquet
    silver_df.write \
        .mode("overwrite") \
        .parquet("s3a://silver-zone/cleaned_stocks/")
        
    print("✅ ETL Job Complete! Check your MinIO Silver Zone.")
    spark.stop()

if __name__ == "__main__":
    main()
