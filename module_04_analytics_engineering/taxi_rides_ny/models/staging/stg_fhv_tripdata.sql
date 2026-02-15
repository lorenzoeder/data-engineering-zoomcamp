with source as (
    select 
        dispatching_base_num,
        Affiliated_base_number,
        pulocationid,
        dolocationid,
        pickup_datetime,
        dropOff_datetime,
        sr_flag
    from {{ source('raw', 'fhv_tripdata') }}
    -- Filter out records with null dispatching_base_num (data quality requirement)
    where dispatching_base_num is not null
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(Affiliated_base_number as string) as affiliated_base_num,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- flags
        cast(sr_flag as string) as sr_flag  -- Shared Ride flag (Y/N)

    from source
)

select
    dispatching_base_num,
    affiliated_base_num,
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    sr_flag
from renamed

{% if target.name == 'dev' %}
-- Sample records for dev environment using deterministic date filter
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}
