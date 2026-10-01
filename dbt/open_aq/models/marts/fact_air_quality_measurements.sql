with final as (
    select
        location_id,
        sensor_id,
        parameter_id,
        value,
        measurement_timestamp,
        loaded_timestamp
    from {{ ref("int__raw_air_quality") }}
)

select * from final