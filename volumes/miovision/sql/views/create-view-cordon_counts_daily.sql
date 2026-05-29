--DROP VIEW miovision_api.cordon_counts_daily;
CREATE OR REPLACE VIEW miovision_api.cordon_counts_daily AS (
    SELECT
        c.camera_group,
        c.label,
        atr.datetime_bin::date AS dt,
        SUM(lat.adjusted_volume) FILTER (WHERE classification_uid IN (1, 4, 5, 8, 9))  AS auto_mode_share,
        SUM(lat.adjusted_volume) FILTER (WHERE classification_uid IN (3))  AS surface_transit_mode_share,
        SUM(lat.adjusted_volume) FILTER (WHERE classification_uid IN (6))  AS ped_mode_share,
        SUM(lat.adjusted_volume) FILTER (WHERE classification_uid IN (10))  AS bike_mode_share
    FROM miovision_api.volumes_15min_atr_unfiltered_table AS atr
    JOIN miovision_api.cordons_long AS c ON
        atr.intersection_uid = c.intersection_uid
        --travelling in the cordon direction
        AND left(atr.dir, 1) = c.exit_leg
        AND (
            atr.leg = c.exit_leg
            --count active modes wherever they are, if they are travelling in right direction
            OR atr.classification_uid IN (6, 10)
        ),
    LATERAL (
        SELECT CASE
            WHEN classification_uid = 1 THEN 1.2
            WHEN classification_uid = 3 THEN
                --peak hour adjustment for bus/streetcar occupancy
                CASE WHEN date_part('hour', atr.datetime_bin) IN (7,8,9,16,17,18) THEN 70
                ELSE 30 END
            ELSE volume END AS adjusted_volume
    ) AS lat
    WHERE classification_uid <> 2 --use bike approaches instead
    GROUP BY
        c.camera_group,
        c.label,
        atr.datetime_bin::date
);

ALTER VIEW miovision_api.cordon_counts_daily OWNER TO miovision_admins;
GRANT SELECT ON TABLE miovision_api.cordon_counts_daily TO bdit_humans;

COMMENT ON VIEW miovision_api.cordon_counts_daily
IS 'Daily cordon counts, with occupancy adjustment of 1.2 for autos, and 70 (peak) / 30 (off-peak) for transit.
Filter camera_group and leg';
