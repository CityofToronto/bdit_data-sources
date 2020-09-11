--to match locations from API to the closest point on VZ map
CREATE TABLE wys.locations_api_vz_updated AS
SELECT
  api.*,
  vz.*,
  ST_Distance(ST_Transform(api.geom_api,2952), ST_Transform(vz.geom_vz,2952)) AS diff
FROM
(SELECT 
DISTINCT (api_id), address, sign_name, 
 REVERSE(SUBSTRING(reverse(sign_name), '([0-9]{1,8})')) AS serial_number, 
 dir, start_date AS dt_api, loc,
st_setsrid(st_makepoint(split_part(regexp_replace(loc, '[()]', '', 'g'), ','::text, 2)::float, 
						split_part(regexp_replace(loc, '[()]', '', 'g'), ','::text, 1)::float), 4326) AS geom_api 
FROM 
 (SELECT DISTINCT ON (api_id,  TRIM(regexp_replace(sign_name, '([0-9]{5,8})',''))) api_id, 
  address, sign_name, dir, start_date, loc  FROM wys.locations
 ORDER BY api_id, TRIM(regexp_replace(sign_name, '([0-9]{5,8})','')), start_date DESC) dist
WHERE Length(SUBSTRING(reverse(sign_name), '([0-9]{1,8})')) > '3') api

CROSS JOIN LATERAL

(SELECT safety_measure_name, dt AS dt_vz, shape AS geom_vz
FROM vz_safety_programs.points_wyss
ORDER BY 
ST_Transform(api.geom_api,2952) <-> ST_Transform(shape,2952)
LIMIT 1) vz

ORDER BY diff

--Another table to verify that the api_id has speed count yesterday
CREATE VIEW wys.locations_api_vz_today_updated AS
SELECT *
FROM wys.locations_api_vz_updated
WHERE api_id IN (SELECT DISTINCT(api_id)
FROM wys.speed_counts_agg
WHERE datetime_bin > NOW()::date - interval '2 day' --because airflow hasn't ran today yet
ORDER BY api_id)

--just so that can plot on QGIS
SELECT * 
INTO wys.locations_20200224
FROM wys.locations_api_vz_today_updated
