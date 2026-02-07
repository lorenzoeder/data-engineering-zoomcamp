# Module 3 homework

## BigQuery Setup

```sql
CREATE OR REPLACE EXTERNAL TABLE `project-a2e7442e-cebd-47b3-bd4.ny_taxi.external_yellow_tripdata_h1_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2026_a2e7442e-cebd-47b3-bd4/yellow_tripdata_2024-*']
);
```

```sql
CREATE OR REPLACE TABLE `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata_h1_2024_non_partitioned` AS
SELECT * FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.external_yellow_tripdata_h1_2024`; 
```

## Question 1

```sql
SELECT count(*)
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata_h1_2024_non_partitioned`;
```

The answer is 20,332,093.

## Question 2

```sql
SELECT count(distinct PULocationID)
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata_h1_2024_non_partitioned`;

SELECT count(distinct PULocationID)
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.external_yellow_tripdata_h1_2024`;
```

The answer is 0 MB for the External Table and 155.12 MB for the Materialized Table.

## Question 3

```sql
SELECT PULocationID
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata_h1_2024_non_partitioned`;

SELECT PULocationID, DOLocationID
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata_h1_2024_non_partitioned`;
```

The answer is that BigQuery is a columnar database, and it only scans the specific columns requested in the query.

## Question 4

```sql
SELECT count(*)
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata_h1_2024_non_partitioned`
WHERE fare_amount = 0;
```

The answer is 8,333.

## Question 5

```sql
CREATE OR REPLACE TABLE `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata_h1_2024_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.external_yellow_tripdata_h1_2024`;
```

The answer is Partition by tpep_dropoff_datetime and Cluster on VendorID.

## Question 6

```sql
SELECT DISTINCT VendorID
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata_h1_2024_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01' AND DATE(tpep_dropoff_datetime) <= '2024-03-15';

SELECT DISTINCT VendorID
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata_h1_2024_non_partitioned`
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01' AND DATE(tpep_dropoff_datetime) <= '2024-03-15';
```

The answer is 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table.

## Question 7

The answer is GCP Bucket.

## Question 8

False.

## Question 9

```sql
SELECT count(*)
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata_h1_2024_non_partitioned`;
```

BigQuery estimates that 0 B will be scanned because the query asks to return a total row count of the table. Since the total row count of a materialised table is stored in its metadata, there is no need to scan any data in order to return it.
