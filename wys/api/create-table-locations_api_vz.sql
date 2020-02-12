--to match locations from API to the closest point on VZ map
CREATE TABLE wys.locations_api_vz AS
SELECT
  api.*,
  vz.*,
  ST_Distance(ST_Transform(api.geom_api,2952), ST_Transform(vz.geom_vz,2952)) AS diff
FROM
(SELECT 
api_id, address, sign_name, 
 REVERSE(SUBSTRING(reverse(locations.sign_name), '([0-9]{1,8})')) AS serial_number, 
 dir, start_date AS dt_api, loc,
st_setsrid(st_makepoint(split_part(regexp_replace(loc, '[()]', '', 'g'), ','::text, 2)::float, 
						split_part(regexp_replace(loc, '[()]', '', 'g'), ','::text, 1)::float), 4326) AS geom_api 
FROM wys.locations
WHERE Length(SUBSTRING(reverse(locations.sign_name), '([0-9]{1,8})')) > '3') api

CROSS JOIN LATERAL

(SELECT safety_measure_name, dt AS dt_vz, shape AS geom_vz
FROM vz_safety_programs.points_wyss
ORDER BY 
ST_Transform(api.geom_api,2952) <-> ST_Transform(shape,2952)
LIMIT 1) vz

ORDER BY diff

--Another table to verify that the api_id has speed count yesterday
CREATE VIEW wys.locations_api_vz_today AS
SELECT *
FROM wys.locations_api_vz
WHERE api_id IN (SELECT DISTINCT(api_id)
FROM wys.speed_counts_agg
WHERE datetime_bin > NOW()::date - interval '1 day'
ORDER BY api_id)

--just so that can plot on QGIS
SELECT * 
INTO wys.locations_20200207
FROM wys.locations_api_vz_today


--to find duplicates of api_id
SELECT api_id 
FROM wys.locations_api_vz
GROUP BY api_id
HAVING COUNT(api_id) >1
ORDER BY api_id

--api_id which has two rows due to change in sign serial number or slight change in location
SELECT * FROM wys.locations_api_vz
WHERE api_id IN (377,1967,2422,2788,2791,2793,2944,4035,4648,4649,
4663,4667,4748,7637,7744,7904,8759,9655,9677,9678,
9684,9723,9764,10290,10457,10461,10765,10766,13095,13128,
13129,13130,13161,13162,22721,22722) 
ORDER BY api_id
