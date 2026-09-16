CREATE TABLE sensor_aggregates (
    device_id TEXT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    min_temperature DOUBLE PRECISION,
    event_count INTEGER
);
