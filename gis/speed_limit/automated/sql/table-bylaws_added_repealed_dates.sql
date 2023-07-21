--only id=302 repealed twice
CREATE VIEW gis.bylaws_added_repealed_dates AS
WITH 
added AS ( 
--2907 rows
SELECT id, bylaw_no, substring(bylaw_no FROM 'Added [0-9]{4}-[0-9]{2}-[0-9]{2}') AS added_detail
	FROM gis.bylaws_2020
	WHERE bylaw_no LIKE '%Added%'	
),
repealed AS (
--1696 rows
SELECT id, bylaw_no, substring(bylaw_no FROM 'Repealed [0-9]{4}-[0-9]{2}-[0-9]{2}') AS repealed_detail
	FROM gis.bylaws_2020
	WHERE bylaw_no LIKE '%Repealed%'
)
SELECT law.id, law.deleted, law.bylaw_no, 
substring(added.added_detail FROM '[0-9]{4}-[0-9]{2}-[0-9]{2}')::date AS added_date,
substring(repealed.repealed_detail FROM '[0-9]{4}-[0-9]{2}-[0-9]{2}')::date AS repealed_date,
law.highway, law.between, law.speed_limit_km_per_h
FROM gis.bylaws_2020 law
LEFT JOIN added USING (id)
LEFT JOIN repealed USING (id)
ORDER BY id