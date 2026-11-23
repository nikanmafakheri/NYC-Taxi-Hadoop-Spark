import sys

current_date = None
total_distance = 0.0
total_trips = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        date, distance, count = line.split("\t")
        distance = float(distance)
        count = int(count)
    except ValueError:
        continue

    if current_date == date:
        total_distance += distance
        total_trips += count
    else:
        if current_date is not None:
            avg_distance = total_distance / total_trips
            print(f"{current_date}\t{total_trips}\t{avg_distance:.2f}")
        current_date = date
        total_distance = distance
        total_trips = count

if current_date is not None:
    avg_distance = total_distance / total_trips
    print(f"{current_date}\t{total_trips}\t{avg_distance:.2f}")