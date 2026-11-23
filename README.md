# NYC Yellow Taxi --- Big Data Analytics Pipeline

An end-to-end, containerized big data pipeline built to process and
analyze **2.55 million+ NYC Yellow Taxi trips** from January 2026.

The project implements two complementary processing architectures:

-   **Batch processing:** Distributed storage and processing with Hadoop
    HDFS, Hadoop Streaming (Python MapReduce), and Apache Spark.
-   **Real-time streaming:** Event ingestion through Apache Kafka
    (KRaft), stream processing with Spark Structured Streaming, and
    interactive visualization with Jupyter Notebook and Plotly.

The infrastructure runs locally with Docker Compose. Separate Compose
profiles isolate the Hadoop and Spark/Kafka services to help manage
local resource consumption.

## Key Highlights

-   Processed **2,551,814 cleaned trip records** through the data
    pipeline.
-   Implemented distributed daily aggregations and trip-duration
    analysis with Hadoop Streaming.
-   Built PySpark analytics for pickup-zone volume, trip duration,
    distance, and fare.
-   Implemented Kafka-based event ingestion using **KRaft mode**,
    without ZooKeeper.
-   Processed Kafka events with Spark Structured Streaming using
    **5-second micro-batches**.
-   Wrote streaming results to Parquet and visualized them through an
    interactive Plotly dashboard.
-   Containerized the infrastructure with Docker Compose.

## Architecture

``` text
                         NYC Yellow Taxi Dataset
                           January 2026 (Parquet)
                                    |
                                    v
                         +----------------------+
                         |   Data Cleaning      |
                         |      Python          |
                         +----------+-----------+
                                    |
                       +------------+------------+
                       |                         |
                       v                         v
              Cleaned CSV / Parquet       Cleaned Parquet
                       |                         |
             +---------+---------+               |
             |                   |               |
             v                   v               v
       +-----------+       +------------+   +-------------+
       |    HDFS   |       |   Kafka    |   | Spark Batch |
       +-----+-----+       |  Producer  |   |  Analytics  |
             |             +------+-----+   +-------------+
             v                    |
       +-------------+            v
       | Hadoop      |      +-------------+
       | Streaming   |      | Kafka       |
       | MapReduce   |      | (KRaft)     |
       +------+------+      +------+------+
              |                    |
              v                    v
       Daily statistics      +-------------+
       & duration buckets    | Spark       |
                             | Structured  |
                             | Streaming   |
                             +------+------+
                                    |
                                    v
                             Parquet Sink
                                    |
                                    v
                         +----------------------+
                         | Jupyter + Plotly     |
                         | Interactive Dashboard|
                         +----------------------+
```

## Tech Stack

  -----------------------------------------------------------------------
  Layer                   Technology              Role
  ----------------------- ----------------------- -----------------------
  Orchestration           Docker Compose          Runs the infrastructure
                                                  with separate `hadoop`
                                                  and `streaming`
                                                  profiles

  Distributed storage     Apache Hadoop HDFS      Stores the cleaned CSV
                          3.2.1                   dataset and MapReduce
                                                  outputs

  Batch processing        Hadoop Streaming        Runs Python mapper and
                                                  reducer jobs across the
                                                  Hadoop cluster

  Batch analytics         Apache Spark 3.5.3 /    Performs Parquet-based
                          PySpark                 filtering and
                                                  pickup-zone
                                                  aggregations

  Message broker          Apache Kafka (KRaft)    Handles taxi-trip
                                                  events without
                                                  ZooKeeper

  Stream processing       Spark Structured        Consumes Kafka events
                          Streaming               and writes processed
                                                  micro-batches to
                                                  Parquet

  Visualization           Jupyter Notebook,       Displays streaming
                          Plotly, ipywidgets      metrics and supports
                                                  interactive filtering
  -----------------------------------------------------------------------

## Repository Structure

``` text
nyc-taxi-bigdata-pipeline/
├── 01_data_cleaning/
│   └── clean_data.py
├── 02_hadoop_mapreduce/
│   ├── mapper.py
│   ├── reducer.py
│   ├── duration_mapper.py
│   └── duration_reducer.py
├── 03_spark_batch/
│   └── spark_batch_analytics.py
├── 04_streaming/
│   ├── producer.py
│   └── spark_streaming.py
├── 05_dashboard/
│   └── realtime_dashboard.ipynb
├── data/
│   ├── raw/
│   ├── cleaned/
│   ├── streaming_output/
│   └── checkpoints/
├── docker-compose.yml
└── hadoop.env
```

  -----------------------------------------------------------------------
  Directory                           Description
  ----------------------------------- -----------------------------------
  `01_data_cleaning/`                 Validates and cleans raw records,
                                      then exports CSV and Parquet
                                      datasets

  `02_hadoop_mapreduce/`              Contains the Python mappers and
                                      reducers for daily statistics and
                                      trip-duration buckets

  `03_spark_batch/`                   Contains the PySpark batch
                                      analytics job

  `04_streaming/`                     Contains the Kafka producer and
                                      Spark Structured Streaming consumer

  `05_dashboard/`                     Contains the Jupyter Notebook
                                      dashboard

  `data/`                             Stores raw, cleaned, streaming
                                      output, and checkpoint data
  -----------------------------------------------------------------------

## Data Preparation

The pipeline starts with the January 2026 NYC TLC Yellow Taxi Parquet
dataset.

The cleaning script normalizes the data and filters invalid records,
including trips with non-positive distances, invalid fares, missing
location identifiers, or non-positive trip durations.

The resulting **2,551,814 cleaned records** are exported in two formats:

-   `data/cleaned/yellow_taxi_clean.parquet` --- used by Spark batch
    analytics.
-   `data/cleaned/yellow_taxi_clean.csv` --- used for HDFS ingestion and
    Kafka event replay.

## Batch Processing

### 1. Hadoop HDFS and Streaming MapReduce

The cleaned CSV file is uploaded to HDFS and processed by two Hadoop
Streaming jobs. The mapper and reducer logic is implemented in Python
and communicates through standard input and output.

**Daily trip volume and average distance**

1.  The mapper extracts the pickup date and trip distance from each
    record.
2.  It emits tab-delimited values containing the date, distance, and a
    count of `1`.
3.  Hadoop shuffles and groups records by date.
4.  The reducer calculates the total trip count and average distance for
    each date.

**Trip-duration distribution**

A second mapper calculates trip duration and assigns each trip to one of
six intervals:

-   `0–5` minutes
-   `5–10` minutes
-   `10–15` minutes
-   `15–30` minutes
-   `30–60` minutes
-   `60+` minutes

The reducer counts the trips in each duration bucket.

**Execution result:** The daily aggregation processed 2,551,815 input
lines (including the CSV header) across 2 map tasks and 1 reduce task,
producing 33 daily summaries in approximately 28 seconds.

### 2. Apache Spark Batch Analytics

The PySpark batch job reads the cleaned Parquet dataset and performs
filtering and aggregation by `PULocationID`.

The analysis retains trips with:

-   `trip_distance > 1.0` mile
-   Trip duration between 1 and 120 minutes

This reduced the dataset from 2,551,814 cleaned records to **1,837,978
filtered trips**.

Selected results:

  --------------------------------------------------------------------------------
  Pickup zone   `PULocationID`        Trips         Avg.         Avg.    Avg. fare
                                                duration     distance 
  ----------- ---------------- ------------ ------------ ------------ ------------
  JFK Airport              132      138,778    38.42 min     15.29 mi      \$61.62

  Midtown                  161       82,377    17.59 min      2.82 mi      \$18.65
  Center                                                              

  Upper East               237       80,588    15.02 min      2.32 mi      \$15.77
  Side South                                                          

  LaGuardia                138       79,864    28.81 min      9.63 mi      \$44.23
  Airport                                                             

  Upper East               236       78,190    15.38 min      2.42 mi      \$16.24
  Side North                                                          
  --------------------------------------------------------------------------------

The results show a difference between airport trips, which tend to be
longer and have higher average fares, and busy Manhattan pickup zones,
which have shorter average trip distances and durations.

## Real-Time Streaming

The streaming workflow replays cleaned historical trip records as
events. It demonstrates a real-time event-processing architecture; it
does **not** ingest live GPS or live TLC trip events.

### 1. Event ingestion with Kafka

`04_streaming/producer.py` reads the cleaned CSV, serializes each trip
as JSON, and publishes events to the Kafka topic:

``` text
taxi-trips
```

The producer sends approximately 20 events per second.

Kafka runs in **KRaft mode**, so the setup does not require ZooKeeper.

### 2. Processing with Spark Structured Streaming

The Spark streaming job:

1.  Connects to Kafka through the internal Docker listener.
2.  Reads JSON event payloads using an explicit schema.
3.  Calculates `trip_duration_min`.
4.  Adds a `processed_at` timestamp.
5.  Processes data with a 5-second micro-batch trigger.
6.  Appends the processed records to a Parquet sink.

Streaming output is written to:

``` text
data/streaming_output/
```

Spark checkpoint data is stored in:

``` text
data/checkpoints/
```

### 3. Interactive dashboard

The Jupyter Notebook dashboard reads the Parquet output and refreshes
its visualizations every 5 seconds.

It includes:

-   **Trip distance distribution:** Histogram of trip distances.
-   **Trip duration trends:** Time-series view of recently processed
    trips.
-   **Top pickup locations:** Bar chart of the 10 most active pickup
    zones.
-   **Fare vs. distance:** Scatter plot for exploring the relationship
    between trip distance and fare.
-   **Interactive explorer:** Filters collected trips by maximum
    distance and passenger count.

## Getting Started

### Prerequisites

-   Docker Engine
-   Docker Compose plugin
-   Python 3
-   The January 2026 NYC Yellow Taxi Parquet dataset

### 1. Clone the repository

``` bash
git clone https://github.com/nikanmafakheri/NYC-Taxi-Hadoop-Spark.git
cd NYC-Taxi-Hadoop-Spark
```

### 2. Create a Python environment

``` bash
python3 -m venv hvenv
source hvenv/bin/activate

pip install pandas pyarrow kafka-python-ng jupyter plotly nbformat ipywidgets
```

### 3. Prepare the dataset

Place the raw January 2026 NYC Yellow Taxi Parquet file in `data/raw/`.

Run the cleaning script:

``` bash
python 01_data_cleaning/clean_data.py
```

The script should generate:

``` text
data/cleaned/yellow_taxi_clean.parquet
data/cleaned/yellow_taxi_clean.csv
```

### 4. Run Hadoop MapReduce jobs

Start the Hadoop services:

``` bash
docker compose --profile hadoop up -d
```

Create the HDFS input directory and upload the cleaned CSV:

``` bash
docker exec -it namenode hdfs dfs -mkdir -p /user/nyc_taxi/input

docker exec -it namenode hdfs dfs -put -f \
  /data/cleaned/yellow_taxi_clean.csv \
  /user/nyc_taxi/input/
```

Run the daily statistics job:

``` bash
docker exec -it namenode hadoop jar \
  /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -files /scripts/mapper.py,/scripts/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /user/nyc_taxi/input/yellow_taxi_clean.csv \
  -output /user/nyc_taxi/output_daily_stats
```

Run the trip-duration distribution job:

``` bash
docker exec -it namenode hadoop jar \
  /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -files /scripts/duration_mapper.py,/scripts/duration_reducer.py \
  -mapper "python3 duration_mapper.py" \
  -reducer "python3 duration_reducer.py" \
  -input /user/nyc_taxi/input/yellow_taxi_clean.csv \
  -output /user/nyc_taxi/output_duration_dist
```

When the Hadoop jobs are complete, stop the Hadoop profile before
starting the Spark/Kafka profile:

``` bash
docker compose --profile hadoop down
```

### 5. Run Spark batch analytics

Create the output and checkpoint directories, then start the streaming
profile:

``` bash
mkdir -p data/streaming_output data/checkpoints
chmod -R 777 data/cleaned data/streaming_output data/checkpoints

docker compose --profile streaming up -d
```

Submit the Spark batch job:

``` bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/batch/spark_batch_analytics.py
```

### 6. Run the streaming pipeline

**Create the Kafka topic:**

``` bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic taxi-trips \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

**Terminal 1 --- Start Spark Structured Streaming:**

``` bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /opt/spark/work-dir/streaming/spark_streaming.py
```

**Terminal 2 --- Start the Kafka producer:**

``` bash
source hvenv/bin/activate
python 04_streaming/producer.py
```

**Terminal 3 --- Launch the dashboard:**

``` bash
source hvenv/bin/activate
jupyter notebook 05_dashboard/realtime_dashboard.ipynb
```

Execute the notebook cells to view the live-refreshing charts and use
the interactive filters.

## Engineering Challenges

Building the pipeline involved solving several practical integration and
infrastructure problems.

### Python compatibility in Hadoop containers

The Hadoop 3.2.1 environment uses an older Python runtime. Python
f-strings caused compatibility issues in the mapper and reducer scripts.
Replacing them with `.format()` string formatting resolved the problem.

### Container filesystem permissions

The Spark container runs as a non-root user, which initially prevented
it from writing to host-mounted output directories. Creating the
required directories and adjusting their permissions allowed Spark to
persist output on the host.

### Kafka networking across Docker and the host

The Kafka producer runs on the host, while Spark runs inside Docker.
They therefore use different Kafka listeners:

  Client                Bootstrap address
  --------------------- -------------------
  Host-based producer   `localhost:9092`
  Containerized Spark   `kafka:29092`

Using the correct listener for each environment resolved the connection
issues.

### Kafka offsets and Spark checkpoints

Using `startingOffsets=latest` caused the streaming job to miss events
published before the consumer finished initializing. Using `earliest`
and persistent checkpointing made the replay workflow more predictable.

## Project Scope and Notes

-   The dataset used in this project is the January 2026 NYC TLC Yellow
    Taxi dataset.
-   The streaming workflow simulates continuous event ingestion by
    replaying historical records through Kafka.
-   Hadoop and Spark/Kafka are started with separate Docker Compose
    profiles to reduce local resource usage.
-   The reported processing times and analytics results are observations
    from this local project environment, not general performance
    benchmarks.

## Author

**Nikan Mafakheri**

-   GitHub: [@nikanmafakheri](https://github.com/nikanmafakheri)

------------------------------------------------------------------------

If you find this project useful or have suggestions, feel free to
explore the repository and share your feedback.
