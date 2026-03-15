# Module 7 Homework

## Question 1

```bash
docker exec -it module_07_streaming-redpanda-1 rpk version
```

rpk version: v25.3.9

## Question 2

```bash
docker exec -it module_07_streaming-redpanda-1 rpk topic create green-trips

uv run python homework/producer.py 
```

took 10.86 seconds

## Question 3

```bash
uv run python homework/consumer_postgres.py
```

```sql
SELECT count(*)
FROM processed_events
WHERE trip_distance > 5;
```

The answer is 8,506

## Question 4

```sql
CREATE TABLE IF NOT EXISTS processed_events_aggregated (
window_start TIMESTAMP(3) NOT NULL,
PULocationID INT NOT NULL,
num_trips BIGINT,
PRIMARY KEY (window_start, PULocationID)
);
```

```bash
docker exec -it module_07_streaming-jobmanager-1 flink run -py /opt/src/job/aggregation_job_homework.py
```

```sql
SELECT PULocationID, num_trips
FROM processed_events_aggregated
ORDER BY num_trips DESC
LIMIT 3;
```

The answer is 74

## Question 5

```sql
CREATE TABLE IF NOT EXISTS processed_events_sessionized (
session_start TIMESTAMP(3) NOT NULL,
session_end TIMESTAMP(3) NOT NULL,
PULocationID INT NOT NULL,
num_trips BIGINT,
PRIMARY KEY (session_start, session_end, PULocationID)
);

CREATE TABLE IF NOT EXISTS processed_events_longest_session (
ranking_key INT PRIMARY KEY,
PULocationID INT,
session_start TIMESTAMP(3),
session_end TIMESTAMP(3),
num_trips BIGINT
);
```

```bash
docker exec -it module_07_streaming-jobmanager-1 flink run -py /opt/src/job/session_window_job_homework.py
```

```sql
SELECT num_trips
FROM processed_events_longest_session
WHERE ranking_key = 1;
```

The answer is 81

## Question 6

```sql
CREATE TABLE IF NOT EXISTS processed_events_hourly_tips (
  window_start TIMESTAMP(3) PRIMARY KEY,
  total_tip_amount DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS processed_events_highest_tip_hour (
  ranking_key INT PRIMARY KEY,
  window_start TIMESTAMP(3),
  total_tip_amount DOUBLE PRECISION
);
```

```bash
docker exec -it module_07_streaming-jobmanager-1 flink run -py /opt/src/job/hourly_tip_amount_job_homework.py
```

```sql
SELECT window_start, total_tip_amount
FROM processed_events_highest_tip_hour
WHERE ranking_key = 1;
```

The answer is 2025-10-16 18:00:00
