from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, avg, desc

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SocialMediaDataAnalysis") \
    .getOrCreate()

# Load CSV file into PySpark DataFrame
file_path = "/mnt/data/social_media_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Display schema of the dataset
df.printSchema()

# Show first few rows
df.show(5)

# Count total records
total_records = df.count()
print(f"Total records: {total_records}")

# Check for missing values
missing_values = df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df.columns])
missing_values.show()

# Basic statistics
df.describe().show()

# Example Transformation: Filtering posts with high engagement (likes > 1000)
if "likes" in df.columns:
    high_engagement_posts = df.filter(col("likes") > 1000)
    print(f"High engagement posts: {high_engagement_posts.count()}")
    high_engagement_posts.show(5)

# Example Aggregation: Average likes per category (if applicable)
if "category" in df.columns and "likes" in df.columns:
    avg_likes_per_category = df.groupBy("category").agg(avg("likes").alias("avg_likes")).orderBy(desc("avg_likes"))
    avg_likes_per_category.show()

# Stop Spark session
spark.stop()
