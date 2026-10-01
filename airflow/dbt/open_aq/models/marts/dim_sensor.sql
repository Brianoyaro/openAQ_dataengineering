with final as (
    select
        sensor_id
    from {{ ref("int__raw_air_quality") }}
)

select * from final


-- dim_sensor
-- ----------------
-- sensor_key
-- sensor_id
-- sensor_name
-- location_id
-- parameter_id
-- ...