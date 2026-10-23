CREATE OR REPLACE VIEW miovision_api.atr_movements_to_pad AS
--vehicle & bike entries
SELECT DISTINCT
    im.intersection_uid,
    im.classification_uid,
    mm.entry_dir AS dir,
    im.leg
FROM miovision_api.intersection_movements AS im
JOIN miovision_api.movement_map AS mm USING (movement_uid, leg)
WHERE
    im.movement_uid <= 4
    AND im.classification_uid IN (1, 2)

UNION

--vehicle & bike exits
SELECT DISTINCT
    im.intersection_uid,
    im.classification_uid,
    mm.exit_dir,
    mm.exit_leg
FROM miovision_api.intersection_movements AS im
JOIN miovision_api.movement_map AS mm USING (movement_uid, leg)
WHERE
    im.movement_uid <= 4
    AND im.classification_uid IN (1, 2)
   
UNION

--pedestrian entries
SELECT DISTINCT
    im.intersection_uid,
    im.classification_uid,
    mm.entry_dir,
    mm.leg
FROM miovision_api.intersection_movements AS im
JOIN miovision_api.movement_map AS mm USING (movement_uid, leg)
WHERE
    im.movement_uid IN (5, 6)
    AND im.classification_uid = 6
    
UNION

--bike approach entries
SELECT DISTINCT
    im.intersection_uid,
    im.classification_uid,
    mm.entry_dir,
    mm.leg
FROM miovision_api.intersection_movements AS im
JOIN miovision_api.movement_map AS mm USING (movement_uid, leg)
WHERE
    im.movement_uid = 7
    AND im.classification_uid = 10
ORDER BY classification_uid, leg, dir;

ALTER VIEW miovision_api.atr_movements_to_pad OWNER TO miovision_admins;
GRANT SELECT ON TABLE miovision_api.atr_movements_to_pad TO bdit_humans, miovision_api_bot;
