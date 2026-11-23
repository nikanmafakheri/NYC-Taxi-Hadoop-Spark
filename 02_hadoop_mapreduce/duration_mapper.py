import sys
from datetime import datetime

FMT = "%Y-%m-%d %H:%M:%S"

def get_bucket(minutes):
    if minutes < 5:
        return "01_0-5_min"
    elif minutes < 10:
        return "02_5-10_min"
    elif minutes < 15:
        return "03_10-15_min"
    elif minutes < 30:
        return "04_15-30_min"
    elif minutes < 60:
        return "05_30-60_min"
    else:
        return "06_60+_min"

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("tpep_pickup_datetime"):
        continue

    parts = line.split(",")
    if len(parts) < 2:
        continue

    try:
        pickup = datetime.strptime(parts[0][:19], FMT)
        dropoff = datetime.strptime(parts[1][:19], FMT)
        duration_min = (dropoff - pickup).total_seconds() / 60.0
        if duration_min > 0:
            print("{}\t1".format(get_bucket(duration_min)))
    except ValueError:
        continue