-- Banc d'essai : modele relationnel normalise equivalent (PostgreSQL 15)
-- Une table de reference des capteurs, une table des mesures.
-- La cle primaire (sensor_id, ts) cree l'index B-tree utilise par les requetes.

CREATE TABLE IF NOT EXISTS sensors (
  sensor_id  varchar(8)  PRIMARY KEY,
  zone       varchar(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS readings (
  sensor_id    varchar(8)  NOT NULL REFERENCES sensors(sensor_id),
  ts           timestamptz NOT NULL,
  temperature  double precision,
  humidity     double precision,
  battery_pct  smallint,
  PRIMARY KEY (sensor_id, ts)
);
