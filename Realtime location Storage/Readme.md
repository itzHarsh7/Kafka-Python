# Setup Documentation

### 1. To Setup the Things, First you should have docker installed on your system.

### 2. Run this command:
`docker compose up --build`

## To Configure the JDBC Sink Connector

**`POST`** this config to `http://localhost:8083/connectors:`
```
{
  "name": "driver-positions-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "driver-positions",
    "connection.url": "jdbc:postgresql://timescaledb:5432/drivers?user=admin&password=admin",
    "auto.create": "true",
    "insert.mode": "insert",
    "pk.mode": "none",
    "table.name.format": "driver_positions",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

## To Create a Table in TimeScale DB:

```
CREATE TABLE driver_positions (
    driver_id TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    ts TIMESTAMPTZ NOT NULL
);

-- Convert into hypertable
SELECT create_hypertable('driver_positions', 'ts');
CREATE INDEX ON driver_positions (driver_id);
```
## Querying Data in TimescaleDB

### 1. Latest location of each driver
```
SELECT DISTINCT ON (driver_id) driver_id, lat, lon, ts
FROM driver_positions
ORDER BY driver_id, ts DESC;
```

### 2. Driver trajectory in last 1 hour
```
SELECT *
FROM driver_positions
WHERE driver_id = 'Driver_001'
  AND ts > now() - interval '1 hour'
ORDER BY ts;
```

