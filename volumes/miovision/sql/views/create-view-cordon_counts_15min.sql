
DROP VIEW miovision_api.cordon_counts_15min;
CREATE OR REPLACE VIEW miovision_api.cordon_counts_15min AS (
    SELECT
        c.camera_group,
        c.label,
        atr.datetime_bin,
        SUM(atr.volume) FILTER (
            WHERE atr.classification_uid IN (1, 4, 5, 8, 9)) AS auto_volume,
        SUM(atr.volume) FILTER (
            WHERE atr.classification_uid IN (3)) AS surface_transit_volume,
        SUM(atr.volume) FILTER (
            WHERE atr.classification_uid IN (6)) AS ped_volume,
        SUM(atr.volume) FILTER (
            WHERE atr.classification_uid IN (10)) AS bike_volume,
        SUM(atr.volume) AS total_volume,
        array_diff(
            cc.intersection_uids,
            ARRAY_AGG(DISTINCT atr.intersection_uid) FILTER (WHERE atr.classification_uid IN (1, 4, 5, 8, 9))
        ) AS auto_intersections_missing,
        array_diff(
            cc.intersection_uids,
            ARRAY_AGG(DISTINCT atr.intersection_uid) FILTER (WHERE atr.classification_uid IN (3))
        ) AS surface_transit_intersections_missing,
        array_diff(
            cc.intersection_uids,
            ARRAY_AGG(DISTINCT atr.intersection_uid) FILTER (WHERE atr.classification_uid IN (6))
        ) AS ped_intersections_missing,
        array_diff(
            cc.intersection_uids,
            ARRAY_AGG(DISTINCT atr.intersection_uid) FILTER (WHERE atr.classification_uid IN (10))
        ) AS bike_intersections_missing,
        array_diff(
            cc.intersection_uids,
            ARRAY_AGG(DISTINCT atr.intersection_uid)
        ) AS total_intersections_missing,
        COUNT(DISTINCT atr.intersection_uid) FILTER (WHERE atr.classification_uid IN (1, 4, 5, 8, 9)) AS auto_intersections_present,
        COUNT(DISTINCT atr.intersection_uid) FILTER (WHERE atr.classification_uid IN (3)) AS surface_transit_intersections_present,
        COUNT(DISTINCT atr.intersection_uid) FILTER (WHERE atr.classification_uid IN (6)) AS ped_intersections_present,
        COUNT(DISTINCT atr.intersection_uid) FILTER (WHERE atr.classification_uid IN (10)) AS bike_intersections_present,
        COUNT(DISTINCT atr.intersection_uid) AS total_intersections_present
    FROM miovision_api.volumes_15min_atr_unfiltered AS atr
    JOIN miovision_api.cordons_long AS c ON
        atr.intersection_uid = c.intersection_uid
        --travelling in the cordon direction
        AND left(atr.dir, 1) = c.exit_leg
        AND (
            atr.leg = c.exit_leg
            --count active modes wherever they are, if they are travelling in right direction
            OR atr.classification_uid IN (6, 10)
        )
        AND atr.datetime_bin >= c.start_date
    --used for the array intersection
    JOIN miovision_api.cordons AS cc USING (camera_group, label)
    WHERE
        atr.classification_uid <> 2 --use bike approaches instead
        AND atr.classification_uid <> 7 --generally not in use
    GROUP BY
        c.camera_group,
        c.label,
        atr.datetime_bin,
        cc.intersection_uids
);

ALTER VIEW miovision_api.cordon_counts_15min OWNER TO miovision_admins;
GRANT SELECT ON TABLE miovision_api.cordon_counts_15min TO bdit_humans;

COMMENT ON VIEW miovision_api.cordon_counts_15min
IS '15 minute cordon counts, with occupancy adjustment of 1.2 for autos, and 70 (peak) / 30 (off-peak) for transit.
Filter camera_group and leg';

/*
--test, 0.2s
SELECT *
FROM miovision_api.cordon_counts_15min
WHERE datetime_bin >= '2026-08-09' AND datetime_bin < '2026-08-10' AND camera_group = 'Bathurst - South of Dupont'
*/