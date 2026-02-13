with green_trips as (
    select *,
          'GREEN' as taxi_type
    from {{ ref('stg_green_tripdata') }}
)

, yellow_trips as (
    select *,
          'YELLOW' as taxi_type
    from {{ ref('stg_yellow_tripdata') }}
)

, trips_unioned as (
    select * from green_trips
    union all
    select * from yellow_trips
)

select * from trips_unioned