CREATE OR REPLACE VIEW miovision_api.volumes_15min_mvt_filtered AS (
    SELECT
        v15.intersection_uid,
        v15.datetime_bin,
        v15.classification_uid,
        v15.leg,
        v15.movement_uid,
        v15.volume
    FROM miovision_api.volumes_15min_mvt_unfiltered AS v15
    --anti join unacceptable_gaps
    LEFT JOIN miovision_api.unacceptable_gaps AS un USING (datetime_bin, intersection_uid)
    --anti join anomalous_ranges
    LEFT JOIN miovision_api.anomalous_ranges AS ar
        ON (ar.problem_level = ANY(ARRAY['do-not-use'::text, 'questionable'::text]))
        AND (
            ar.intersection_uid = v15.intersection_uid
            OR ar.intersection_uid IS NULL
        ) AND (
            ar.classification_uid = v15.classification_uid
            OR ar.classification_uid IS NULL
        ) AND (
            ar.leg = v15.leg
            OR ar.leg IS NULL
        )
        AND v15.datetime_bin >= ar.range_start
        AND (
            v15.datetime_bin <= ar.range_end
            OR ar.range_end IS NULL
        )
    WHERE
        ar.uid IS NULL
        AND un.datetime_bin IS NULL
);

ALTER VIEW miovision_api.volumes_15min_mvt_filtered OWNER TO miovision_admins;

COMMENT ON VIEW miovision_api.volumes_15min_mvt_filtered IS E''
'TMC style 15-minute Miovision volume view with anomalous_ranges labeled '
'''do-not-use'' or ''questionable'' filtered out, unacceptable_gaps anti-joined,
and only common (>0.05%) movements (`intersection_movements`) included.';

GRANT SELECT ON TABLE miovision_api.volumes_15min_mvt_filtered TO bdit_humans;
