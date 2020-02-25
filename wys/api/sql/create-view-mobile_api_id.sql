DROP VIEW wys.mobile_api_id CASCADE;

CREATE MATERIALIZED VIEW wys.mobile_api_id AS

SELECT row_number() OVER (ORDER BY installation_date, ward_no) AS location_id,
a.ward_no, a.location, a.from_street, a.to_street, a.direction, 
a.installation_date, a.removal_date, a.comments, a.combined,
b.api_id
FROM (
SELECT *, 
 CASE WHEN new_sign_number NOT LIKE 'W%' THEN 'Ward ' || ward_no || ' - S' || new_sign_number 
 WHEN new_sign_number LIKE 'W%' AND new_sign_number LIKE '% - S%' THEN 'Ward ' || SUBSTRING(new_sign_number, 2, 10)
 WHEN new_sign_number LIKE 'W%' AND new_sign_number NOT LIKE '% - S%' THEN 'Ward ' || SUBSTRING(new_sign_number, 2, POSITION(' - ' IN new_sign_number) + 1 ) || 'S' || RIGHT(new_sign_number, 1)
 END AS combined
FROM wys.mobile_sign_installations
) a 
LEFT JOIN 
(SELECT DISTINCT(api_id), sign_name
FROM wys.locations
WHERE sign_name LIKE 'Ward%'
ORDER BY sign_name) b
ON a.combined = b.sign_name