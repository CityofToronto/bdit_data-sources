DROP MATERIALIZED VIEW wys.mobile_api_id;

CREATE MATERIALIZED VIEW wys.mobile_api_id AS

WITH a AS (
   SELECT
      ward_no,
      location,
      from_street,
      to_street,
      direction,
      installation_date,
      removal_date,
      new_sign_number,
      comments,
      id,
      CASE 
         WHEN new_sign_number NOT LIKE 'W%' 
            THEN 'Ward ' || ward_no || ' - S' || new_sign_number 
         WHEN new_sign_number LIKE 'W%' 
            AND new_sign_number LIKE '% - S%' 
            THEN 'Ward ' || SUBSTRING(new_sign_number, 2, 10)
         WHEN new_sign_number LIKE 'W%' 
            AND new_sign_number NOT LIKE '% - S%' 
            THEN 'Ward ' || SUBSTRING(new_sign_number, 2, POSITION(' - ' IN new_sign_number) + 1 ) || 
               'S' || RIGHT(new_sign_number, 1)
         END AS combined
   FROM wys.mobile_sign_installations
), 

b AS (
   SELECT DISTINCT 
      api_id, 
      sign_name, 
      start_date,
      lead(start_date) OVER w AS next_start,
      lag(start_date) OVER w AS prev_start
   FROM wys.locations
   WHERE sign_name LIKE 'Ward%'
   WINDOW w AS (
      PARTITION BY sign_name 
      ORDER BY start_date
   )
)

SELECT 
   id AS location_id,
   a.ward_no, 
   a.location, 
   a.from_street, 
   a.to_street, 
   a.direction, 
   a.installation_date, 
   a.removal_date, 
   a.comments, 
   a.combined,
   b.api_id
FROM a 
LEFT JOIN b
ON a.combined = b.sign_name
   AND (b.prev_start IS NULL OR installation_date >= b.prev_start)
   AND (b.next_start IS NULL OR installation_date < b.start_date);

CREATE UNIQUE INDEX ON wys.mobile_api_id (location_id);