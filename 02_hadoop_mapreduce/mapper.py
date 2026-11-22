import sys 

def mapper():
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startwith("tpep_pickup_datetime"):
            continue

    parts = line.split(",")
    if len (parts) < 4:
            continue

    try:
        date = parts[0].split(" ")[0]
        distance = float(parts[3])
        print(f"{date}\t{distance}")
    except ValueError:
            continue
            