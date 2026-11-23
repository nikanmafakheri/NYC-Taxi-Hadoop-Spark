import csv
import json 
import time
from kafka import KafkaProducer


TOPIC = "taxi-trips"
CSV_PATH = "data/cleaned/yellow_taxi_clean.csv"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),

)
with open(CSV_PATH, "r") as f:
    reader = csv.DictReader(f)
    for i , row in enumerate(reader , start=1):
        event = {
            "tpep_pickup_datetime": row["tpep_pickup_datetime"],
            "tpep_dropoff_datetime": row["tpep_dropoff_datetime"],
            "passenger_count": int(float(row["passenger_count"])),
            "trip_distance": float(row["trip_distance"]),
            "PULocationID": int(float(row["PULocationID"])),
            "DOLocationID": int(float(row["DOLocationID"])),
            "fare_amount": float(row["fare_amount"]),
            "total_amount": float(row["total_amount"]),
        }
        producer.send(TOPIC, value=event)

        if i % 20 == 0:
            producer.flush()
            print(f"Sent {i} events...")
            time.sleep(1)