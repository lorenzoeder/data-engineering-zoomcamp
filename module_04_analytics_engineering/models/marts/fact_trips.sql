with trips_unioned as (
    select *
    from {{ ref('int_trips_unioned_clean') }}
)

select * from trips_unioned