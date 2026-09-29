with source as (
    select
        location_id,
        location_name,
        sensor_id,
        parameter_id,
        unit,
        value,
        measurement_timestamp,
        loaded_timestamp
    from {{ source("openaq", "raw_air_quality") }}
)

select * from source