SELECT COUNT(*) AS mesures FROM iot_migre.readings_by_sensor_day;
SELECT COUNT(*) AS alertes FROM iot_migre.alerts_by_zone_day;
SELECT ts, temperature, humidity FROM iot_migre.readings_by_sensor_day
  WHERE sensor_id = 'S017' AND day = '2026-03-01' LIMIT 3;
