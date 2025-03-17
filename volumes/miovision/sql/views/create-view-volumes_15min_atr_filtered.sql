DROP VIEW miovision_api.volumes_15min_atr_filtered;
CREATE VIEW miovision_api.volumes_15min_atr_filtered AS (

    --entries
    SELECT
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        v15.leg,
        mmm.entry_dir AS dir,
        SUM(v15.volume) AS volume
    FROM miovision_api.volumes_15min_mvt_filtered AS v15
    JOIN miovision_api.movement_map AS mmm USING (movement_uid, leg)
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
        SUM(v15.volume) AS volume
    FROM miovision_api.volumes_15min_mvt_filtered AS v15
    JOIN miovision_api.movement_map AS mmm USING (movement_uid, leg)
    WHERE mmm.exit_leg IS NOT NULL
    GROUP BY
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        mmm.exit_leg,
        mmm.exit_dir
);

ALTER VIEW miovision_api.volumes_15min_atr_filtered OWNER TO miovision_admins;

COMMENT ON VIEW miovision_api.volumes_15min_atr_filtered IS E''
'A ATR style transformation of miovision_api.volumes_15min_mvt with anomalous_ranges labeled '
'''do-not-use'' or ''questionable'' filtered out, unacceptable_gaps anti-joined, 
and only common (>0.05%) movements (`intersection_movements`) included.';

GRANT SELECT ON TABLE miovision_api.volumes_15min_atr_filtered TO bdit_humans;
