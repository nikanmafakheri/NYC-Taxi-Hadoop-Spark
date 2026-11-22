import pandas as pd
from pathlib import Path

RAW_PATH = Path("data/raw/yellow_tripdata_2026-01.parquet")
CLEAN_CSV = Path("data/cleaned/yellow_taxi_clean.csv")
CLEAN_PARQUET = Path("data/cleaned/yellow_taxi_clean.parquet")

cols = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
]

print(f"Reading raw data from {RAW_PATH}")
df = pd.read_parquet(RAW_PATH, columns=cols)
initial_rows = len(df)


df = df.dropna()

df = df [
    (df["passenger_count"] > 0) &  
    (df["trip_distance"] > 0) & 
    (df["fare_amount"] > 0) & 
    (df["total_amount"] > 0) &
    (df["trip_distance"] <= 100)]

df["passenger_count"] = df["passenger_count"].astype(int)

print(f"rows before cleaning: {initial_rows}, rows after cleaning: {len(df)}")
print(f"rows removed during cleaning: {initial_rows - len(df)}")

df.to_parquet(CLEAN_PARQUET, index=False)
df.to_csv(CLEAN_CSV, index=False)
print (f"Cleaned data saved to {CLEAN_PARQUET} and {CLEAN_CSV}")