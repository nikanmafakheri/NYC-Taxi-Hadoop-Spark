import sys

current_bucket = None
total_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        bucket, count = line.split("\t")
        count = int(count)
    except ValueError:
        continue

    if current_bucket == bucket:
        total_count += count
    else:
        if current_bucket is not None:
            print("{}\t{}".format(current_bucket, total_count))
        current_bucket = bucket
        total_count = count

if current_bucket is not None:
    print("{}\t{}".format(current_bucket, total_count))