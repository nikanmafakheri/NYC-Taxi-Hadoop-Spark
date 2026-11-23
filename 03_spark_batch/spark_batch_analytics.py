from pyspark.sql import SparkSession
from pyspark.sql.functions import col , unix_timestamp , avg , count , round as spark_round

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Batch-Analytics")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/data/cleaned/yellow_taxi_clean.parquet")

df_with_duration = df.withColumn(
    "trip_duration_min",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
    )

filtered_df = df_with_duration.filter(
    (col("trip_distance") > 1.0) &
    (col("trip_duration_min") >= 1.0) &
    (col("trip_duration_min") <= 120.0)
)

print(f"Total rows loaded:   {df.count():,}")
print(f"Rows after filter:   {filtered_df.count():,}")

avg_duration_by_loc = (
    filtered_df.groupBy("PULocationID")
    .agg(
        count("*").alias("total_trips"),
        spark_round(avg("trip_duration_min"), 2).alias("avg_duration_min"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance_miles"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare_usd"),
    )
    .orderBy(col("total_trips").desc())
)

print("\nTop 20 Pickup Locations by Trip Volume (with Avg Duration):")
avg_duration_by_loc.show(20, truncate=False)

avg_duration_by_loc.coalesce(1).write.mode("overwrite").option("header", True).csv("/data/cleaned/spark_location_stats")

spark.stop()