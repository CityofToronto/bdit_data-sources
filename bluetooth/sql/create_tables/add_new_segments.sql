CREATE table mohan.bt_segments_new AS 

WITH lookup AS (
	SELECT 		analysis_id, 
				from_id, 
				origin.int_id AS source, 
				to_id, 
				dest.int_id AS target
	FROM 		mohan.new_added_detectors 
	INNER JOIN 	mohan.bluetooth_nodes origin ON from_id = origin.bluetooth_id 
	INNER JOIN 	mohan.bluetooth_nodes dest ON to_id = dest.bluetooth_id)

, results AS (
	SELECT 		* 
	FROM 		lookup
	CROSS JOIN 	LATERAL pgr_dijkstra('SELECT id, source, target, cost FROM gis.centreline_routing_directional inner join gis.centreline on geo_id = id
												where fcode != 207001', source::int, target::int, TRUE))
, 
lines as (
	SELECT 		analysis_id, street, direction, from_street, to_street, edge AS geo_id, geom 
	FROM 		results			 
	INNER JOIN 	gis.centreline ON edge=geo_id
	INNER JOIN 	mohan.new_added_detectors USING (analysis_id)
	ORDER BY 	analysis_id
)

SELECT 			analysis_id, 
				street, 
				direction, 
				from_street, 
				to_street,
				geom, 
				ST_Length(ST_transform(geom, 2592)) as length
FROM (	SELECT 	analysis_id, street, direction, from_street, to_street, 
				gis.twochar_direction(gis.direction_from_line(ST_linemerge(ST_union(geom)))) AS geom_dir,
				ST_linemerge(ST_union(geom)) AS geom
		FROM lines
		GROUP BY analysis_id, street, direction, from_street, to_street) a;


alter table mohan.bt_segments_new 
add column end_street text;

update  mohan.bt_segments_new 
set end_street = end_street_name
from mohan.routes 
where routes.analysis_id = bt_segments_new.analysis_id;

alter table mohan.bt_segments_new 
add column segment_name text;

update mohan.bt_segments_new 
set segment_name = concat(from_name,  '_', to_name) 
from mohan.new_added_detectors 
where new_added_detectors.analysis_id = bt_segments_new.analysis_id;

alter table mohan.bt_segments_new 
add column bluetooth boolean;

alter table mohan.bt_segments_new 
add column wifi boolean;

update mohan.bt_segments_new 
set bluetooth = TRUE;

update mohan.bt_segments_new -- only true for segments on highway 
set bluetooth = FALSE; 

alter table mohan.bt_segments_new 
add column duplicate boolean;

alter table mohan.bt_segments_new 
add column reversed boolean;

update mohan.bt_segments_new 
set duplicate = FALSE; -- not sure what this is

update mohan.bt_segments_new 
set reversed = FALSE; -- false cause geoms are in the correct directional

