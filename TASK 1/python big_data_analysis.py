from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import urllib.request

# Initialize Spark Session
spark = SparkSession.builder.appName("BigDataAnalysis").getOrCreate()

# Download dataset locally
local_file = "yellow_tripdata_2021-01.csv"
urllib.request.urlretrieve("https://raw.githubusercontent.com/DataTalksClub/nyc-tlc-data/main/yellow_tripdata_2021-01.csv", local_file)

# Load Large Dataset
df = spark.read.csv(local_file, header=True, inferSchema=True)

# Show schema and first few rows
df.printSchema()
df.show(5)

# Basic Statistics
print("Total Rows:", df.count())

# Data Cleaning: Removing Null Values
df = df.dropna()

# Aggregation: Average Trip Distance by Payment Type
agg_df = df.groupBy("payment_type").agg(avg("trip_distance").alias("avg_distance")).orderBy(desc("avg_distance"))
agg_df.show()

# Convert to Pandas for Visualization
pandas_df = agg_df.toPandas()

# Bar Chart: Average Trip Distance by Payment Type
plt.figure(figsize=(12, 6))
sns.barplot(x='payment_type', y='avg_distance', data=pandas_df)
plt.title("Average Trip Distance by Payment Type")
plt.xlabel("Payment Type")
plt.ylabel("Average Distance")
plt.show()

# Line Chart: Trend of Trip Distances
trip_df = df.select("tpep_pickup_datetime", "trip_distance").toPandas()
trip_df['tpep_pickup_datetime'] = pd.to_datetime(trip_df['tpep_pickup_datetime'])
trip_df.set_index('tpep_pickup_datetime', inplace=True)
trip_df.resample('D').mean().plot(figsize=(12, 6), title="Average Trip Distance Over Time")
plt.xlabel("Date")
plt.ylabel("Trip Distance")
plt.show()

# Pie Chart: Payment Type Distribution
payment_counts = df.groupBy("payment_type").count().toPandas()
plt.figure(figsize=(8, 8))
plt.pie(payment_counts['count'], labels=payment_counts['payment_type'], autopct='%1.1f%%', colors=['blue', 'orange', 'green', 'red'])
plt.title("Payment Type Distribution")
plt.show()

# Advanced Chart: Fare Amount vs. Trip Distance (Scatter Plot)
fare_trip_df = df.select("fare_amount", "trip_distance").toPandas()
sns.scatterplot(x=fare_trip_df['trip_distance'], y=fare_trip_df['fare_amount'], alpha=0.5)
plt.title("Fare Amount vs. Trip Distance")
plt.xlabel("Trip Distance")
plt.ylabel("Fare Amount")
plt.show()

# Stop Spark Session
spark.stop()
