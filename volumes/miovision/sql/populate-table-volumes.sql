TRUNCATE miovision.volumes;

INSERT INTO miovision.volumes (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
SELECT B.intersection_uid, (A.datetime_bin AT TIME ZONE 'America/Toronto') AS datetime_bin, C.classification_uid, A.entry_dir_name as leg, D.movement_uid, A.volume
FROM miovision.raw_data A
INNER JOIN miovision.intersections B ON regexp_replace(A.study_name,'Yong\M','Yonge') = B.intersection_name
INNER JOIN miovision.movements D USING (movement)
INNER JOIN miovision.classifications C USING (classification, location_only)
-- WHERE A.lat = B.lat AND A.lng = B.lng
ORDER BY (A.datetime_bin AT TIME ZONE 'America/Toronto'), B.intersection_uid, C.classification_uid, A.entry_name, D.movement_uid;