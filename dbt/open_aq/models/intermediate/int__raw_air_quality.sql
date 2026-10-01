with parameters as (
    select * from {{ ref("stg__parameters")}}
),
raw_air_quality as (
    select * from {{ ref("stg__raw_air_quality")}}
),
final as (
    select
        r.location_id as location_id,
        r.location_name as location_name,
        r.sensor_id as sensor_id,
        p.parameter_id as parameter_id,
        p.parameter_name as parameter_name,
        p.parameter_units as parameter_units,
        p.parameter_display_name as parameter_display_name,
        p.parameter_description as parameter_description,
        r.value as value,
        r.measurement_timestamp as measurement_timestamp,
        r.loaded_timestamp as loaded_timestamp
    from raw_air_quality r
    join parameters p
    on r.parameter_id = p.parameter_id
)

select * from final