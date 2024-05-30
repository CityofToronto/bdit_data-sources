CREATE VIEW miovision_api.volumes_15min_filtered AS (
    SELECT
        v15.volume_15min_uid,
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        v15.leg,
        v15.dir,
        v15.volume
    FROM miovision_api.volumes_15min AS v15
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

COMMENT ON TABLE miovision_api.volumes_15min_filtered IS E''
'miovision_api.volumes_15min with anomalous_ranges labeled '
'''do-not-use'' or ''questionable'' filtered out.';

GRANT SELECT ON TABLE miovision_api.volumes_15min_filtered TO bdit_humans;