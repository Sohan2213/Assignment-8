# Load NYC taxi data into DataFrame
df = spark.read.format("parquet").load("dbfs:/path/to/nyc_taxi_data.parquet")

# Query 1: Add 'Revenue' column
df = df.withColumn("Revenue", df['Fare_amount'] + df['Extra'] + df['MTA_tax'] + df['Improvement_surcharge'] + df['Tip_amount'] + df['Tolls_amount'] + df['Total_amount'])

# Query 2: Count total passengers by area
passengers_by_area = df.groupBy("Pickup_area").agg({"passenger_count": "sum"}).withColumnRenamed("sum(passenger_count)", "Total_passengers")

# Query 3: Real-time average fare/total earning by vendors
vendor_earnings = df.groupBy("VendorID").agg({"Revenue": "avg"}).withColumnRenamed("avg(Revenue)", "Avg_revenue")

# Query 4: Moving count of payments by payment mode
payment_mode_count = df.groupBy("payment_type").count()

# Query 5: Highest two earning vendors on a particular date with passengers and total distance
highest_earning_vendors = df.filter(df['trip_date'] == 'specific_date').groupBy("VendorID").agg({"Revenue": "sum", "passenger_count": "sum", "trip_distance": "sum"}).orderBy("sum(Revenue)", ascending=False).limit(2)

# Query 6: Most number of passengers between two locations
passengers_route = df.groupBy("Pickup_location", "Dropoff_location").agg({"passenger_count": "sum"}).withColumnRenamed("sum(passenger_count)", "Total_passengers").orderBy("Total_passengers", ascending=False).limit(1)

# Query 7: Top pickup locations with most passengers in last 5/10 seconds
from pyspark.sql.functions import current_timestamp, col
recent_pickups = df.filter(col("pickup_time") >= current_timestamp() - expr("INTERVAL 10 SECONDS")).groupBy("Pickup_location").agg({"passenger_count": "sum"}).orderBy("sum(passenger_count)", ascending=False)
