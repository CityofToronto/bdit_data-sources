CREATE VIEW open_data.volumes_atr_cyclists_permanent_daily AS
SELECT centreline_id, direction, location, class_type, datetime_bin::date AS dt, SUM(volume_15min) AS volume_daily 
FROM open_data.volumes_atr_cyclists_permanent
GROUP BY centreline_id, direction, location, class_type, datetime_bin::date
HAVING COUNT(1) = 96
ORDER BY centreline_id, direction, datetime_bin::date desc