import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("tpep_pickup_datetime"):
        continue

    parts = line.split(",")
    if len(parts) < 4:
        continue

    try:
        date = parts[0].split(" ")[0]
        distance = float(parts[3])
        print("{}\t{}\t1".format(date, distance))
    except ValueError:
        continue