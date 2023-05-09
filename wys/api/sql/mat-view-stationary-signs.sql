DROP MATERIALIZED VIEW wys.stationary_signs CASCADE;

CREATE MATERIALIZED VIEW wys.stationary_signs
TABLESPACE pg_default
AS
WITH distinctish_signs AS (
    SELECT DISTINCT ON (
        loc.api_id, 
        (regexp_replace(loc.sign_name, '([0-9]{5,8})'::text, ''::text))
    ) 
        loc.api_id,
        loc.address,
        regexp_replace(loc.sign_name, '([0-9]{5,8})'::text, ''::text) AS sign_name,
        loc.id AS sign_id,
        loc.dir,
        loc.start_date,
        substring(loc.sign_name from '([0-9]{5,8})'::text) AS serial_num,
        st_setsrid(
            st_makepoint(
                split_part(
                    regexp_replace(loc.loc, '[()]'::text, ''::text, 'g'::text), 
                    ','::text, 2)::double precision, 
                split_part(
                    regexp_replace(loc.loc, '[()]'::text, ''::text, 'g'::text), 
                    ','::text, 1)::double precision
            ), 
            4326) AS geom
    FROM wys.locations loc
    WHERE length("substring"(reverse(loc.sign_name), '([0-9]{1,8})'::text)) > 3
    ORDER BY 
        loc.api_id, 
        (regexp_replace(loc.sign_name, '([0-9]{5,8})'::text, ''::text)),
        loc.start_date
)
 
SELECT 
    loc.api_id,
    loc.sign_id,
    loc.address,
    loc.sign_name,
    loc.dir,
    loc.start_date,
	loc.serial_num,
    lead(loc.start_date) OVER w AS next_start,
    lag(loc.start_date) OVER w AS prev_start,
    loc.geom
FROM distinctish_signs loc
JOIN gis.toronto_boundary AS tor ON 
    st_intersects(
        st_buffer(tor.geom::geography, 20), 
        loc.geom::geography)
WINDOW w AS (
    PARTITION BY loc.api_id 
    ORDER BY start_date);

CREATE INDEX ON wys.stationary_signs USING gist(geom);
ANALYZE wys.stationary_signs;
CREATE UNIQUE INDEX ON wys.stationary_signs (sign_id);