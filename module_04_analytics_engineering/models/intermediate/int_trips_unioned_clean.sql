with int_trips_unioned as (
    select *
    from {{ ref('int_trips_unioned') }}
)

, add_uuid as (
    select row_number() over (order by pickup_datetime, pickup_location_id, vendor_id, fare_amount) as trip_id,
           *
    from int_trips_unioned
)

select * from add_uuid