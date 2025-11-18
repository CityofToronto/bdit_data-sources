/*
Transform unfiltered Miovision 15min TMC into ATR
*/

CREATE OR REPLACE VIEW miovision_api.volumes_15min_atr_unfiltered AS

--entries
SELECT
    v15.intersection_uid,
    v15.datetime_bin,
    v15.classification_uid,
    v15.leg,
    mmm.entry_dir AS dir,
    v15.volume
FROM miovision_api.volumes_15min_mvt_unfiltered AS v15
JOIN miovision_api.movement_map AS mmm USING (movement_uid, leg)

UNION ALL --there are no duplicate entries and exits

--exits 
SELECT
    v15.intersection_uid,
    v15.datetime_bin,
    v15.classification_uid,
    mmm.exit_leg AS leg,
    mmm.exit_dir AS dir,
    v15.volume
FROM miovision_api.volumes_15min_mvt_unfiltered AS v15
JOIN miovision_api.movement_map AS mmm USING (movement_uid, leg)
WHERE mmm.exit_leg IS NOT NULL;

ALTER VIEW miovision_api.volumes_15min_atr_unfiltered OWNER TO miovision_admins;

COMMENT ON VIEW miovision_api.volumes_15min_atr_unfiltered
IS 'Transforms miovision TMC-format data to ATR-style data';
