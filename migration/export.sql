-- Export PostgreSQL vers des fichiers CSV, un fichier par table Cassandra.
-- Les jointures et les agregations sont faites ici, une fois pour toutes.
-- Periode migree : du 15/10/2025 au 14/10/2026.

SET TIME ZONE 'UTC';

-- 1. Mesures, regroupees par (capteur, jour) dans Cassandra
\copy (SELECT sensor_id, ts::date, to_char(ts, 'YYYY-MM-DD HH24:MI:SS') || '+0000', temperature, humidity, battery_pct FROM readings WHERE ts >= '2025-10-15' AND ts < '2026-10-15') TO '/tmp/readings.csv' WITH (FORMAT csv)

-- 2. Alertes : la jointure avec sensors apporte la zone
\copy (SELECT s.zone, r.ts::date, to_char(r.ts, 'YYYY-MM-DD HH24:MI:SS') || '+0000', r.sensor_id, r.temperature FROM readings r JOIN sensors s ON s.sensor_id = r.sensor_id WHERE r.temperature > 35 AND r.ts >= '2025-10-15' AND r.ts < '2026-10-15') TO '/tmp/alerts.csv' WITH (FORMAT csv)

-- 3. Indicateurs journaliers : l'agregation est calculee a l'export
\copy (SELECT sensor_id, ts::date, count(*), min(temperature), max(temperature), round(avg(temperature)::numeric, 2) FROM readings WHERE ts >= '2025-10-15' AND ts < '2026-10-15' GROUP BY sensor_id, ts::date) TO '/tmp/daily_stats.csv' WITH (FORMAT csv)

-- 4. Derniere mesure de chaque capteur
\copy (SELECT s.sensor_id, to_char(r.ts, 'YYYY-MM-DD HH24:MI:SS') || '+0000', r.temperature, r.humidity FROM sensors s CROSS JOIN LATERAL (SELECT ts, temperature, humidity FROM readings WHERE sensor_id = s.sensor_id ORDER BY ts DESC LIMIT 1) r) TO '/tmp/last_reading.csv' WITH (FORMAT csv)
