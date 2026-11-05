// Import des fichiers CSV dans les tables Cassandra (commande COPY de cqlsh)
COPY iot_migre.readings_by_sensor_day (sensor_id, day, ts, temperature, humidity, battery_pct) FROM '/tmp/readings.csv';
COPY iot_migre.alerts_by_zone_day (zone, day, ts, sensor_id, temperature) FROM '/tmp/alerts.csv';
COPY iot_migre.daily_stats_by_sensor (sensor_id, day, nb_mesures, temp_min, temp_max, temp_moy) FROM '/tmp/daily_stats.csv';
COPY iot_migre.last_reading_by_sensor (sensor_id, ts, temperature, humidity) FROM '/tmp/last_reading.csv';
