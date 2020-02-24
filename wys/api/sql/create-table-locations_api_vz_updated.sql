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
WHERE datetime_bin > NOW()::date - interval '2 day' --because today is Monday
ORDER BY api_id)

--just so that can plot on QGIS
SELECT * 
INTO wys.locations_20200224
FROM wys.locations_api_vz_today_updated


--to find duplicates of api_id from those that actually work the day before
SELECT api_id 
FROM wys.locations_api_vz_today_updated
GROUP BY api_id
HAVING COUNT(api_id) >1
ORDER BY api_id

--api_id which has two rows due to change in sign serial number or slight change in location
SELECT * FROM wys.locations_api_vz_updated
WHERE api_id IN (1967,2422,2788,2791,2793,2944,4035,4649,4663,7637,
				 7904,8212,8214,8759,9655,9677,9678,9684,9717,9719,
				 9723,9756,9764,9774,10290,10461,10765,10766,13095,13128,
				 13129,13130,13158,13159,13160,13161,13162,13492,13798,22721,
				 22722,22729) 
ORDER BY api_id