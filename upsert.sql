INSERT INTO raw_air_quality (
    location_id,
    location_name,
    sensor_id,
    parameter,
    unit,
    value,
    measurement_timestamp
)
VALUES (
    %(location_id)s,
    %(location_name)s,
    %(sensor_id)s,
    %(parameter)s,
    %(unit)s,
    %(value)s,
    %(measurement_timestamp)s
)
ON CONFLICT (
    sensor_id,
    parameter,
    measurement_timestamp
) DO NOTHING;