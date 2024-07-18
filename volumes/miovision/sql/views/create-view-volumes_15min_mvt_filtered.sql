CREATE VIEW miovision_api.volumes_15min_mvt_filtered AS (
    SELECT
        v15.volume_15min_mvt_uid,
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        v15.leg,
        v15.movement_uid,
        v15.volume,
        v15.processed
    FROM miovision_api.volumes_15min_mvt AS v15
    LEFT JOIN miovision_api.anomalous_ranges AS ar
        ON ar.problem_level = ANY(ARRAY['do-not-use', 'questionable'])
        AND ar.intersection_uid = v15.intersection_uid
        AND (
            ar.classification_uid = v15.classification_uid
            OR ar.classification_uid IS NULL
        ) AND (
            ar.leg = v15.leg
            OR ar.leg IS NULL
        ) AND v15.datetime_bin >= ar.range_start
        AND (
            v15.datetime_bin <= ar.range_end
            OR ar.range_end IS NULL
        )
    WHERE ar.uid IS NULL
);

COMMENT ON VIEW miovision_api.volumes_15min_mvt_filtered IS E''
'miovision_api.volumes_15min_mvt with anomalous_ranges labeled '
'''do-not-use'' or ''questionable'' filtered out.';

GRANT SELECT ON TABLE miovision_api.volumes_15min_mvt_filtered TO bdit_humans;