DROP VIEW miovision_api.miovision_15min_atr_filtered;
CREATE VIEW miovision_api.miovision_15min_atr_filtered AS (

    --entries
    SELECT
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        v15.leg,
        mmm.entry_dir AS dir,
        SUM(v15.volume) AS volume
    FROM miovision_api.volumes_15min_mvt_filtered AS v15
    JOIN miovision_api.miovision_movement_map_new AS mmm USING (movement_uid, leg)
    GROUP BY
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        v15.leg,
        mmm.entry_dir

    UNION

    --exits 
    SELECT
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        mmm.exit_leg,
        mmm.exit_dir,
        SUM(v15.volume)
    FROM miovision_api.volumes_15min_mvt_filtered AS v15
    JOIN miovision_api.miovision_movement_map_new AS mmm USING (movement_uid, leg)
    GROUP BY
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        mmm.exit_leg,
        mmm.exit_dir
);

--test: 0.2 s with primary key
SELECT *
FROM miovision_api.miovision_15min_atr_filtered
WHERE
    intersection_uid = 6
    AND classification_uid = 1
    AND datetime_bin = '2024-06-25 12:00:00'
    AND leg <> LEFT(dir, 1);

--DR i0627 test using new view
--41s for original, 1:06 for view (1.3M rows)
SELECT
    volumes.intersection_uid,
    date_trunc('hour', volumes.datetime_bin) AS datetime_bin,
    volumes.leg,
    volumes.dir,
    classifications.classification,
    SUM(volumes.volume) AS volume
FROM miovision_api.miovision_15min_atr_filtered AS volumes
INNER JOIN miovision_api.classifications USING (classification_uid)
WHERE
    volumes.classification_uid NOT IN (2, 7)
    AND volumes.datetime_bin >= '2024-01-01'
    AND volumes.datetime_bin < '2024-05-22'
    AND volumes.intersection_uid IN (76, 88, 81, 85, 84, 87, 75, 83, 86, 49, 80, 36)
GROUP BY
    volumes.intersection_uid,
    classifications.classification,
    date_trunc('hour', volumes.datetime_bin),
    volumes.leg,
    volumes.dir;