TRUNCATE miovision.classifications;

INSERT INTO miovision.classifications(classification, location_only)
SELECT classification, (CASE WHEN movement IN ('ccw','cw') THEN 1 ELSE 0 END)::boolean AS location_only
FROM miovision.raw_data
GROUP BY classification, CASE WHEN movement IN ('ccw','cw') THEN 1 ELSE 0 END
ORDER BY CASE WHEN movement IN ('ccw','cw') THEN 1 ELSE 0 END, COUNT(*) desc;