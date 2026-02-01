# Module 2 homework

## Question 1

After upload to GCS bucket, the answer is 134.5 MB

![alt text](<screengrabs/Screenshot 2026-02-01 at 15.22.55.png>)

## Question 2

The answer is `green_tripdata_2020-04.csv`

![alt text](<screengrabs/Screenshot 2026-02-01 at 15.31.08.png>)

## Question 3

Querying the merged `yellow_tripdata` BigQuery table:

```sql
SELECT count(*)
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata` 
WHERE filename like '%2020%'
;
```

The answer is 24,648,499.

## Question 4

Querying the merged `green_tripdata` BigQuery table:

```sql
SELECT count(*)
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.green_tripdata` 
WHERE filename like '%2020%'
;
```

The answer is 1,734,051.

## Question 5

Querying the merged `yellow_tripdata` BigQuery table:

```sql
SELECT count(*)
FROM `project-a2e7442e-cebd-47b3-bd4.ny_taxi.yellow_tripdata` 
WHERE filename = 'yellow_tripdata_2021-03.csv'
;
```

The answer is 1,925,152.

## Question 6

The answer is adding a `timezone` property to the `trigger.Schedule` task, set to `America/New_York`

![alt text](<screengrabs/Screenshot 2026-02-01 at 16.01.06.png>)
