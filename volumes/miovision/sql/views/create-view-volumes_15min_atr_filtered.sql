CREATE OR REPLACE VIEW miovision_api.volumes_15min_atr_filtered AS

SELECT
    v15.intersection_uid,
    v15.datetime_bin,
    v15.classification_uid,
    mmm.exit_leg,
    mmm.exit_dir,
    SUM(v15.volume) AS volume
FROM miovision_api.volumes_15min_atr_unfiltered AS v15
LEFT JOIN miovision_api.unacceptable_gaps AS un USING (datetime_bin, intersection_uid)
WHERE
    un.datetime_bin IS NULL
    AND NOT EXISTS ( --anti join anomalous_ranges
        SELECT 1
        FROM miovision_api.anomalous_ranges AS ar
        WHERE
            ar.problem_level = ANY(ARRAY['do-not-use'::text, 'questionable'::text])
            AND (
                ar.intersection_uid = v15.intersection_uid
                OR ar.intersection_uid IS NULL
            ) AND (
                ar.classification_uid = v15.classification_uid
                OR ar.classification_uid IS NULL
            )
            -- leg is ignored here
            -- any anomalousness on a leg removes all ATR counts
            AND v15.datetime_bin >= ar.range_start
            AND (
                v15.datetime_bin <= ar.range_end
                OR ar.range_end IS NULL
            )
);

ALTER VIEW miovision_api.volumes_15min_atr_filtered OWNER TO miovision_admins;

COMMENT ON VIEW miovision_api.volumes_15min_atr_filtered IS E''
'A ATR style transformation of miovision_api.volumes_15min_mvt with anomalous_ranges labeled '
'''do-not-use'' or ''questionable'' filtered out, unacceptable_gaps anti-joined, 
and only common (>0.05%) movements (`intersection_movements`) included.';

GRANT SELECT ON TABLE miovision_api.volumes_15min_atr_filtered TO bdit_humans;
