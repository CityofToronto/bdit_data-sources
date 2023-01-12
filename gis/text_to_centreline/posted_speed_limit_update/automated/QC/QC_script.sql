-- this query finds all of the streets that have intersecting geometry
SELECT ST_Transform(ST_SetSRID(p1.geom::geometry, 26917), 4326), ST_Transform(ST_SetSRID(p2.geom::geometry, 26917), 4326), 
p1.id_orig AS id, p1.street_name, p1.extents, p1.speed_limit, p2.id_orig AS id, p2.street_name, p2.extents, p2.speed_limit
FROM (SELECT DISTINCT ON (street_name, extents) p.*, o."Speed_Limit_km_per_hr" speed_limit, o."ID" id_orig FROM crosic."posted_speed_limit_xml_open_data_withQC" p JOIN posted_speed_limit_xml_open_data_original o ON p.street_name = o."Highway" AND p.extents = o."Between") AS p1, 
(SELECT DISTINCT ON (street_name, extents) p.*, o."Speed_Limit_km_per_hr" speed_limit, o."ID" id_orig FROM crosic."posted_speed_limit_xml_open_data_withQC" p JOIN posted_speed_limit_xml_open_data_original o ON p.street_name = o."Highway" AND p.extents = o."Between") AS p2
where p1.geom is not null and p2.geom is not null and st_overlaps(p1.geom::geometry, p2.geom::geometry)
and p1.index > p2.index
and st_geometrytype(p1.geom::geometry) <> 'ST_MultiLineString' and st_geometrytype(p2.geom::geometry) <> 'ST_MultiLineString';


-- this query finds all of the bylaws where there is more than one street that has a name 
-- with a low lev dist within the buffered line in between the intersections
select ST_Transform(ST_SetSRID(geom::geometry, 26917), 4326), * from crosic."posted_speed_limit_xml_open_data_withQC"
where street_name_arr LIKE '%,%'

-- look at streets with low confidence values 
SELECT ST_Transform(ST_SetSRID(geom::geometry, 26917), 4326) geom_transformed, * 
FROM crosic."posted_speed_limit_xml_open_data_withQC"
WHERE confidence LIKE '%Low%6%' OR confidence LIKE '%Low%7%' and "ratio" is not NULL




