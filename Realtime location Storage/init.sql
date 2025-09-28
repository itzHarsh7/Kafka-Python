-- init.sql
CREATE TABLE driver_positions (
    driver_id TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    ts TIMESTAMPTZ NOT NULL
);

SELECT create_hypertable('driver_positions', 'ts');
CREATE INDEX ON driver_positions (driver_id);
