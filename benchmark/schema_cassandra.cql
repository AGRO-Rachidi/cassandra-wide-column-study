// Banc d'essai : meme modele que l'etude de cas, sur un noeud unique (RF = 1)
// Difference avec le schema de la section 8 : pas de TTL, car on conserve tout l'historique.

CREATE KEYSPACE IF NOT EXISTS iot_bench
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE iot_bench;

CREATE TABLE IF NOT EXISTS readings_by_sensor_day (
  sensor_id text,
  day date,
  ts timestamp,
  temperature double,
  humidity double,
  battery_pct int,
  PRIMARY KEY ((sensor_id, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE IF NOT EXISTS last_reading_by_sensor (
  sensor_id text PRIMARY KEY,
  ts timestamp,
  temperature double,
  humidity double
);

CREATE TABLE IF NOT EXISTS alerts_by_zone_day (
  zone text,
  day date,
  ts timestamp,
  sensor_id text,
  temperature double,
  PRIMARY KEY ((zone, day), ts, sensor_id)
) WITH CLUSTERING ORDER BY (ts DESC, sensor_id ASC);

CREATE TABLE IF NOT EXISTS daily_stats_by_sensor (
  sensor_id text,
  day date,
  nb_mesures int,
  temp_min double,
  temp_max double,
  temp_moy double,
  PRIMARY KEY (sensor_id, day)
) WITH CLUSTERING ORDER BY (day DESC);
