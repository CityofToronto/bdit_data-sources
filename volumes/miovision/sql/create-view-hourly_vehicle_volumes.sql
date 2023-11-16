DROP VIEW gwolofs.hourly_vehicle_volumes;
CREATE VIEW gwolofs.hourly_vehicle_volumes AS 

SELECT
    v15.intersection_uid,
    i.intersection_name,
    cm.centreline_id,
    date_trunc('hour', v15.datetime_bin) AS hr,
    v15.leg,
    v15.dir,
    SUM(v15.volume) AS vehicle_volume
--atr style 15min summarization
FROM miovision_api.volumes_15min AS v15
--get centreline_id
LEFT JOIN miovision_api.centreline_miovision AS cm USING(intersection_uid, leg)
--get intersection information
LEFT JOIN miovision_api.intersections AS i USING (intersection_uid)
--group all types of vehicles
INNER JOIN miovision_api.classifications AS c ON
    c.classification_uid = v15.classification_uid
    AND c.class_type = 'Vehicles'
--anti join holidays
LEFT JOIN ref.holiday ON holiday.dt = v15.datetime_bin::date
--anti join anomalous_ranges table
LEFT JOIN miovision_api.anomalous_ranges AS ar ON
    ar.intersection_uid = v15.intersection_uid
    AND (
        ar.classification_uid = v15.classification_uid
        OR ar.classification_uid IS NULL
    )
    AND v15.datetime_bin >= LOWER(ar.time_range)
    AND v15.datetime_bin < UPPER(ar.time_range)
WHERE
    v15.volume IS NOT NULL --null volumes are data gaps
    AND date_part('isodow', v15.datetime_bin) <= 5 --only weekdays
    AND holiday.holiday IS NULL --exclude holidays
    AND ar.problem_level IS NULL --exclude anomalous date ranges
GROUP BY
    hr,
    v15.intersection_uid,
    i.intersection_name,
    cm.centreline_id,
    v15.leg,
    v15.dir
HAVING
    SUM(v15.volume) > 0
    AND COUNT(DISTINCT v15.datetime_bin) = 4 --full hour
ORDER BY
    hr,
    v15.intersection_uid,
    v15.leg,
    v15.dir;
    
--test, 40s. Seq scan over entire volumes_15min table. 
SELECT *
FROM gwolofs.hourly_vehicle_volumes
WHERE
    hr >= '2023-11-01'::timestamp
    AND hr < '2023-11-02'::timestamp;