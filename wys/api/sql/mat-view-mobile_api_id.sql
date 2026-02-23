DROP MATERIALIZED VIEW wys.mobile_api_id;

CREATE MATERIALIZED VIEW wys.mobile_api_id AS

WITH sign_locations AS (
    SELECT DISTINCT 
        loc.api_id,
        snc.sign_name_clean AS sign_name,
        loc.start_date,
        lead(loc.start_date) OVER w AS next_start,
        lag(loc.start_date) OVER w AS prev_start
    FROM wys.locations AS loc,
        LATERAL(
            SELECT initcap((regexp_match(sign_name, '[Ww]ard \d{1,2} - [Ss]\d{1,2}'))[1]) AS sign_name_clean
        ) AS snc
    WHERE regexp_like(sign_name, '[Ww]ard \d{1,2} - [Ss]\d{1,2}')
    WINDOW w AS (PARTITION BY snc.sign_name_clean ORDER BY start_date)
),

mobile_installations AS (
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
                THEN 'Ward ' || ward_no || ' - S'
                || regexp_substr(new_sign_number, '\d{1,2}')
            WHEN new_sign_number LIKE 'W%' AND new_sign_number LIKE '% - S%'
                THEN 'Ward ' || SUBSTRING(new_sign_number, 2, 10)
            WHEN new_sign_number LIKE 'W%' AND new_sign_number NOT LIKE '% - S%'
                THEN 'Ward '
                || SUBSTRING(new_sign_number, 2, POSITION(' - ' IN new_sign_number) + 1)
                || 'S' || RIGHT(new_sign_number, 1)
        END AS combined
    FROM wys.mobile_sign_installations
)

SELECT 
    msi.id AS location_id,
    msi.ward_no,
    msi.location,
    msi.from_street,
    msi.to_street,
    msi.direction,
    msi.installation_date,
    msi.removal_date,
    msi.comments,
    msi.combined,
    b.api_id
FROM mobile_installations AS msi
LEFT JOIN sign_locations AS b ON 
    msi.combined = b.sign_name
    AND (b.prev_start IS NULL OR msi.installation_date >= b.start_date)
    --if there's a subsequent sign, don't match installations after the subsequent sign's start_date
    AND (b.next_start IS NULL OR msi.installation_date < b.next_start);

CREATE UNIQUE INDEX ON wys.mobile_api_id (location_id);