CREATE TABLE raw_air_quality (
    location_id          BIGINT NOT NULL,
    location_name        TEXT NOT NULL,
    sensor_id            BIGINT NOT NULL,
    parameter            TEXT NOT NULL,
    unit                 TEXT NOT NULL,
    value                DOUBLE PRECISION NOT NULL,
    measurement_timestamp TIMESTAMPTZ NOT NULL,
    loaded_timestamp     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT raw_air_quality_pk
        PRIMARY KEY (
            sensor_id,
            parameter,
            measurement_timestamp
        )
);