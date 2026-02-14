with int_trips_unioned as (
    select *
    from {{ ref('int_trips_unioned') }}
)

, add_uuid as (
    select {{ dbt_utils.generate_surrogate_key(["vendor_id", "pickup_datetime", "pickup_location_id", "taxi_type"]) }} as trip_id,
           *
    from int_trips_unioned
)

select * from add_uuid
qualify row_number() over (partition by vendor_id, pickup_datetime, pickup_location_id, taxi_type order by dropoff_datetime) = 1