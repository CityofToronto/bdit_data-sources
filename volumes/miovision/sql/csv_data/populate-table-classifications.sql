TRUNCATE miovision_api.classifications;

INSERT INTO miovision_api.classifications(classification, location_only)
SELECT classification, (CASE WHEN movement IN ('ccw','cw') THEN 1 ELSE 0 END)::boolean AS location_only
FROM miovision_csv.raw_data
GROUP BY classification, CASE WHEN movement IN ('ccw','cw') THEN 1 ELSE 0 END
ORDER BY CASE WHEN movement IN ('ccw','cw') THEN 1 ELSE 0 END, COUNT(*) desc;

---add zero_padded by classification_uid
UPDATE miovision_api.classifications
SET zero_padded = CASE
    WHEN classification_uid IN (1,2,6,10) THEN TRUE
	ELSE FALSE
END

--- add class_type by classification_uid
UPDATE miovision_api.classifications
SET class_type = CASE
    WHEN clasification_uid IN (6) THEN 'Pedestrians'
    WHEN clasification_uid IN (2,7,10) THEN 'Cyclist'
	ELSE 'Vehicles'
END