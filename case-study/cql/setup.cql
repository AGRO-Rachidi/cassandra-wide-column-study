CREATE KEYSPACE IF NOT EXISTS iot
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};

USE iot;

DROP TABLE IF EXISTS readings_by_sensor_day;
DROP TABLE IF EXISTS last_reading_by_sensor;
DROP TABLE IF EXISTS alerts_by_zone_day;
DROP TABLE IF EXISTS daily_stats_by_sensor;

CREATE TABLE readings_by_sensor_day (
  sensor_id text,
  day date,
  ts timestamp,
  temperature double,
  humidity double,
  battery_pct int,
  PRIMARY KEY ((sensor_id, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS',
                    'compaction_window_size': '1'};

CREATE TABLE last_reading_by_sensor (
  sensor_id text PRIMARY KEY,
  ts timestamp,
  temperature double,
  humidity double
);

CREATE TABLE alerts_by_zone_day (
  zone text,
  day date,
  ts timestamp,
  sensor_id text,
  temperature double,
  PRIMARY KEY ((zone, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE daily_stats_by_sensor (
  sensor_id text,
  day date,
  nb_mesures int,
  temp_min double,
  temp_max double,
  temp_moy double,
  PRIMARY KEY (sensor_id, day)
) WITH CLUSTERING ORDER BY (day DESC);

-- S001: three measures with BATCH keeping last_reading in sync
BEGIN BATCH
  INSERT INTO readings_by_sensor_day (sensor_id, day, ts, temperature, humidity, battery_pct)
    VALUES ('S001', '2026-10-05', '2026-10-05 08:00:00+0000', 27.4, 61.0, 88);
  INSERT INTO last_reading_by_sensor (sensor_id, ts, temperature, humidity)
    VALUES ('S001', '2026-10-05 08:00:00+0000', 27.4, 61.0);
APPLY BATCH;

BEGIN BATCH
  INSERT INTO readings_by_sensor_day (sensor_id, day, ts, temperature, humidity, battery_pct)
    VALUES ('S001', '2026-10-05', '2026-10-05 08:00:10+0000', 27.6, 60.8, 88);
  INSERT INTO last_reading_by_sensor (sensor_id, ts, temperature, humidity)
    VALUES ('S001', '2026-10-05 08:00:10+0000', 27.6, 60.8);
APPLY BATCH;

BEGIN BATCH
  INSERT INTO readings_by_sensor_day (sensor_id, day, ts, temperature, humidity, battery_pct)
    VALUES ('S001', '2026-10-05', '2026-10-05 08:00:20+0000', 27.9, 60.5, 88);
  INSERT INTO last_reading_by_sensor (sensor_id, ts, temperature, humidity)
    VALUES ('S001', '2026-10-05 08:00:20+0000', 27.9, 60.5);
APPLY BATCH;

-- S002: normal then alert
BEGIN BATCH
  INSERT INTO readings_by_sensor_day (sensor_id, day, ts, temperature, humidity, battery_pct)
    VALUES ('S002', '2026-10-05', '2026-10-05 08:00:00+0000', 30.1, 55.2, 91);
  INSERT INTO last_reading_by_sensor (sensor_id, ts, temperature, humidity)
    VALUES ('S002', '2026-10-05 08:00:00+0000', 30.1, 55.2);
APPLY BATCH;

BEGIN BATCH
  INSERT INTO readings_by_sensor_day (sensor_id, day, ts, temperature, humidity, battery_pct)
    VALUES ('S002', '2026-10-05', '2026-10-05 08:00:10+0000', 36.4, 52.0, 91);
  INSERT INTO last_reading_by_sensor (sensor_id, ts, temperature, humidity)
    VALUES ('S002', '2026-10-05 08:00:10+0000', 36.4, 52.0);
  INSERT INTO alerts_by_zone_day (zone, day, ts, sensor_id, temperature)
    VALUES ('Zone-Nord', '2026-10-05', '2026-10-05 08:00:10+0000', 'S002', 36.4);
APPLY BATCH;

INSERT INTO daily_stats_by_sensor (sensor_id, day, nb_mesures, temp_min, temp_max, temp_moy)
  VALUES ('S001', '2026-10-03', 8640, 24.9, 31.2, 27.8);
INSERT INTO daily_stats_by_sensor (sensor_id, day, nb_mesures, temp_min, temp_max, temp_moy)
  VALUES ('S001', '2026-10-04', 8640, 25.1, 32.0, 28.1);
INSERT INTO daily_stats_by_sensor (sensor_id, day, nb_mesures, temp_min, temp_max, temp_moy)
  VALUES ('S001', '2026-10-05', 3, 27.4, 27.9, 27.63);

-- Keyspace RF=1 to demonstrate partitioning
CREATE KEYSPACE IF NOT EXISTS iot_part
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

CREATE TABLE IF NOT EXISTS iot_part.readings_demo (
  sensor_id text,
  ts timestamp,
  temperature double,
  PRIMARY KEY (sensor_id, ts)
);

INSERT INTO iot_part.readings_demo (sensor_id, ts, temperature) VALUES ('S001', '2026-10-05 08:00:00+0000', 27.4);
INSERT INTO iot_part.readings_demo (sensor_id, ts, temperature) VALUES ('S002', '2026-10-05 08:00:00+0000', 30.1);
INSERT INTO iot_part.readings_demo (sensor_id, ts, temperature) VALUES ('S003', '2026-10-05 08:00:00+0000', 22.0);
INSERT INTO iot_part.readings_demo (sensor_id, ts, temperature) VALUES ('S004', '2026-10-05 08:00:00+0000', 25.5);
INSERT INTO iot_part.readings_demo (sensor_id, ts, temperature) VALUES ('S005', '2026-10-05 08:00:00+0000', 28.2);
INSERT INTO iot_part.readings_demo (sensor_id, ts, temperature) VALUES ('S006', '2026-10-05 08:00:00+0000', 26.7);
