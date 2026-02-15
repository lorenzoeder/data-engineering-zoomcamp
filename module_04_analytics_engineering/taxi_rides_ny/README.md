# Module 4 homework

## Question 1

The answer is int_trips_unioned only. It would need + on either side of the model name to also build dependencies.

## Question 2

The answer is that dbt fails the test with non-zero exit code.

## Question 3

```shell
python -c 'import duckdb; conn = duckdb.connect("taxi_rides_ny.duckdb"); print(conn.execute("SELECT count(*) FROM prod.fct_monthly_zone_revenue").fetchdf())'
```

The answer is 12,184.

## Question 4

```shell
python -c "import duckdb; conn = duckdb.connect('taxi_rides_ny.duckdb'); print(conn.execute(\"SELECT pickup_zone, sum(revenue_monthly_total_amount) FROM prod.fct_monthly_zone_revenue WHERE service_type = 'Green' AND date_trunc('year', revenue_month) = '2020-01-01' GROUP BY 1 ORDER BY 2 DESC LIMIT 1\").fetchdf())"
```

The answer is East Harlem North.

## Question 5

```shell
python -c "import duckdb; conn = duckdb.connect('taxi_rides_ny.duckdb'); print(conn.execute(\"SELECT sum(total_monthly_trips) FROM prod.fct_monthly_zone_revenue WHERE service_type = 'Green' AND revenue_month = '2019-10-01'\").fetchdf())"
```

The answer is 384,624.

## Question 6

```shell
dbt run --select stg_fhv_tripdata.sql --target prod
```

```shell
python -c "import duckdb; conn = duckdb.connect('taxi_rides_ny.duckdb'); print(conn.execute(\"SELECT count(*) FROM prod.stg_fhv_tripdata\").fetchdf())"
```

The answer is 43,244,693.
