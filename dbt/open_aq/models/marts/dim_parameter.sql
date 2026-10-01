with final as (
    select
        parameter_id,
        parameter_name,
        parameter_units,
        parameter_display_name,
        parameter_description
    from {{ ref("int__raw_air_quality") }}
)

select * from final