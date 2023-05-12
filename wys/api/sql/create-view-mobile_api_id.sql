DROP VIEW wys.mobile_api_id CASCADE;

CREATE MATERIALIZED VIEW wys.mobile_api_id AS

WITH sign_locations AS (
    SELECT DISTINCT 
        api_id,
        sign_name,
        start_date,
        lead(start_date) OVER w AS next_start,
        lag(start_date) OVER w AS prev_start
    FROM wys.locations
    WHERE sign_name LIKE 'Ward %'
    WINDOW w AS (PARTITION BY sign_name ORDER BY start_date)
)

SELECT 
    a.id AS location_id,
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
FROM (
    SELECT
        msi.ward_no,
        msi.location,
        msi.from_street,
        msi.to_street,
        msi.direction,
        msi.installation_date,
        msi.removal_date,
        msi.new_sign_number,
        msi.comments,
        msi.id, 
        CASE WHEN msi.new_sign_number NOT LIKE 'W%'
            THEN 'Ward ' || msi.ward_no || ' - S' || msi.new_sign_number
            WHEN msi.new_sign_number LIKE 'W%' AND msi.new_sign_number LIKE '% - S%'
            THEN 'Ward ' || SUBSTRING(msi.new_sign_number, 2, 10)
        WHEN msi.new_sign_number LIKE 'W%' AND msi.new_sign_number NOT LIKE '% - S%' 
            THEN 'Ward '
            || SUBSTRING(msi.new_sign_number, 2, POSITION(' - ' IN msi.new_sign_number) + 1)
            || 'S' || RIGHT(msi.new_sign_number, 1)
        END AS combined
    FROM wys.mobile_sign_installations AS msi
) AS a 
LEFT JOIN sign_locations AS b
ON a.combined = b.sign_name
   AND (b.prev_start IS NULL OR msi.installation_date >= b.prev_start)
   AND (b.next_start IS NULL OR msi.installation_date < b.start_date);

CREATE UNIQUE INDEX ON  wys.mobile_api_id (location_id);