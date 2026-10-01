with source as (
    select
        parameter_id,
        parameter_name,
        parameter_units,
        parameter_display_name,
        parameter_description
    from {{ source("openaq", "parameters") }}
)

select * from source