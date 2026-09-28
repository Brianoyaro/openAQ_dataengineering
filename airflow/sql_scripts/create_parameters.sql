CREATE TABLE IF NOT EXISTS parameters (
    parameter_id          BIGINT PRIMARY KEY,
    parameter_name        TEXT NOT NULL,
    parameter_units       TEXT NOT NULL,
    parameter_display_name TEXT NULL,
    parameter_description TEXT NULL
);
