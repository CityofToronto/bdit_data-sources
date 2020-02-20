DROP MATERIALIZED VIEW wys.stationary_signs CASCADE;

CREATE MATERIALIZED VIEW wys.stationary_signs AS
	WITH distinctish_signs AS(
	SELECT DISTINCT ON (api_id,  regexp_replace(sign_name, '([0-9]{5,8})','')) 
	api_id, address, regexp_replace(sign_name, '([0-9]{5,8})','') AS sign_name, dir, start_date, st_setsrid(st_makepoint(split_part(regexp_replace(loc, '[()]', '', 'g'), ','::text, 2)::float, 
						split_part(regexp_replace(loc, '[()]', '', 'g'), ','::text, 1)::float), 4326) AS geom
	FROM wys.locations
	WHERE length("substring"(reverse(locations.sign_name), '([0-9]{1,8})'::text)) > 3
	ORDER BY api_id, regexp_replace(sign_name, '([0-9]{5,8})',''))
	
	SELECT 
	api_id, 
	row_number() OVER (ORDER BY api_id, start_date) AS sign_id,
	address,
	sign_name,
	dir,
	start_date,
	lead(start_date) OVER w as next_start,
	lag(start_date) OVER w as prev_start,
	geom
	FROM distinctish_signs
	WINDOW w as (PARTITION BY api_id order by start_date);

CREATE INDEX ON wys.stationary_signs USING gist(geom);
ANALYZE wys.stationary_signs;