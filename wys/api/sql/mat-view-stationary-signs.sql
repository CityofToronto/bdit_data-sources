DROP MATERIALIZED VIEW wys.stationary_signs CASCADE;

CREATE MATERIALIZED VIEW wys.stationary_signs
TABLESPACE pg_default
AS
 WITH distinctish_signs AS (
         SELECT DISTINCT ON (api_id, (regexp_replace(sign_name, '([0-9]{5,8})'::text, ''::text))) api_id,
            address,
            regexp_replace(sign_name, '([0-9]{5,8})'::text, ''::text) AS sign_name,
	 		id as sign_id,
            dir,
            start_date,
	 		substring(sign_name from '([0-9]{5,8})'::text) as serial_num,
            st_setsrid(st_makepoint(split_part(regexp_replace(loc, '[()]'::text, ''::text, 'g'::text), ','::text, 2)::double precision, split_part(regexp_replace(loc, '[()]'::text, ''::text, 'g'::text), ','::text, 1)::double precision), 4326) AS geom
           FROM wys.locations
          WHERE length("substring"(reverse(sign_name), '([0-9]{1,8})'::text)) > 3
          ORDER BY api_id, (regexp_replace(sign_name, '([0-9]{5,8})'::text, ''::text)),
	  				start_date
        )
 SELECT api_id,
     sign_id,
    address,
    sign_name,
    dir,
    start_date,
	serial_num,
    lead(start_date) OVER w AS next_start,
    lag(start_date) OVER w AS prev_start,
    geom
   FROM distinctish_signs
   WINDOW w as (PARTITION BY api_id order by start_date);

CREATE INDEX ON wys.stationary_signs USING gist(geom);
ANALYZE wys.stationary_signs;
CREATE UNIQUE INDEX ON wys.stationary_signs (sign_id);