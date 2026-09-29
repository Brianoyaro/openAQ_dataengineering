with parameters as (
    select * from {{ ref("stg__parameters")}}
),
raw_air_quality as (
    select * from {{ ref("stg__raw_air_quality")}}
),
final as (
    select
        r.location_id,
        r.location_name,
        r.sensor_id,
        p.parameter_id,
        p.parameter_name,
        p.parameter_units,
        p.parameter_display_name,
        p.parameter_description,
        r.value,
        r.measurement_timestamp,
        r.loaded_timestamp
    from raw_air_quality r
    join parameters p
    on r.parameter_id = p.parameter_id
)

select * from final