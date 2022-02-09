CREATE OR REPLACE FUNCTION wys.update_stationary_signs (IN _mon DATE)
RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

WITH distinctish_signs AS (
    SELECT DISTINCT ON (locations.api_id, (regexp_replace(locations.sign_name, '([0-9]{5,8})'::text, ''::text))) locations.api_id,
        locations.address,
        regexp_replace(locations.sign_name, '([0-9]{5,8})'::text, ''::text) AS sign_name,
        locations.id AS sign_id,
        locations.dir,
        locations.start_date,
        "substring"(locations.sign_name, '([0-9]{5,8})'::text) AS serial_num,
        st_setsrid(st_makepoint(split_part(regexp_replace(locations.loc, '[()]'::text, ''::text, 'g'::text), ','::text, 2)::double precision, split_part(regexp_replace(locations.loc, '[()]'::text, ''::text, 'g'::text), ','::text, 1)::double precision), 4326) AS geom
    FROM wys.locations
    WHERE length("substring"(reverse(locations.sign_name), '([0-9]{1,8})'::text)) > 3
    ORDER BY locations.api_id, (regexp_replace(locations.sign_name, '([0-9]{5,8})'::text, ''::text)), locations.start_date
)
INSERT INTO wys.stationary_signs
SELECT distinctish_signs.api_id,
    distinctish_signs.sign_id,
    distinctish_signs.address,
    distinctish_signs.sign_name,
    distinctish_signs.dir,
    distinctish_signs.start_date,
    distinctish_signs.serial_num,
    lead(distinctish_signs.start_date) OVER w AS next_start,
    lag(distinctish_signs.start_date) OVER w AS prev_start,
    distinctish_signs.geom
FROM distinctish_signs
WHERE start_date >= _mon
WINDOW w AS (PARTITION BY distinctish_signs.api_id ORDER BY distinctish_signs.start_date);
$BODY$;

REVOKE EXECUTE ON FUNCTION wys.update_stationary_signs (DATE)FROM public;
GRANT EXECUTE ON FUNCTION wys.update_stationary_signs (DATE) TO wys_bot;