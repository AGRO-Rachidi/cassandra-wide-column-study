SET TIME ZONE 'UTC';
SELECT count(*) AS mesures FROM readings WHERE ts >= '2025-10-15' AND ts < '2026-10-15';
SELECT count(*) AS alertes FROM readings WHERE temperature > 35 AND ts >= '2025-10-15' AND ts < '2026-10-15';
SELECT ts, temperature, humidity FROM readings
  WHERE sensor_id = 'S017' AND ts >= '2026-03-01' AND ts < '2026-03-02' ORDER BY ts DESC LIMIT 3;
