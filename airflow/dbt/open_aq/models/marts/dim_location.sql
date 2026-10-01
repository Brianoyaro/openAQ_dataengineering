with final as (
    select
        location_id,
        location_name
    from {{ ref("int__raw_air_quality") }}
)

select * from final


-- dim_location
-- ----------------
-- location_key
-- location_id
-- location_name
-- city
-- county
-- country
-- latitude
-- longitude