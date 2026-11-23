from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = (
    SparkSession.builder
    .appName("NYC_Taxi_Realtime_Streaming")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "taxi-trips")
    .option("startingOffsets", "earliest")
    .load()
)

parsed_stream = (
    raw_stream.selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
    .withColumn(
        "trip_duration_min",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0
    )
    .withColumn("processed_at", current_timestamp())
)

file_query = (
    parsed_stream.writeStream
    .format("parquet")
    .option("checkpointLocation", "/data/checkpoints")
    .option("path", "/data/streaming_output")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

console_query = (
    parsed_stream.select("PULocationID", "trip_distance", "trip_duration_min", "fare_amount")
    .writeStream
    .format("console")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

spark.streams.awaitAnyTermination()