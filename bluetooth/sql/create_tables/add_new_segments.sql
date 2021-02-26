CREATE table mohan.bt_segments_new AS (
WITH lookup AS (
SELECT analysis_id, from_id, origin.int_id AS source, to_id, dest.int_id AS target
FROM mohan.new_added_detectors 
INNER JOIN mohan.bluetooth_nodes origin ON from_id = origin.bluetooth_id 
INNER JOIN mohan.bluetooth_nodes dest ON to_id = dest.bluetooth_id
),
results AS (
	SELECT * 
	FROM lookup
			 CROSS JOIN LATERAL pgr_dijkstra('SELECT id, source, target, cost FROM gis.centreline_routing_directional inner join gis.centreline on geo_id = id
where fcode != 207001', source::int, target::int, TRUE)		 
), 
lines as (
	SELECT analysis_id, street, direction, from_street, to_street, edge AS geo_id, geom 
	FROM results			 
INNER JOIN gis.centreline ON edge=geo_id
INNER JOIN mohan.new_added_detectors USING (analysis_id)
ORDER BY analysis_id
)
SELECT analysis_id, street, direction, from_street, to_street,
	CASE WHEN geom_dir != direction THEN ST_reverse(geom) 
	ELSE geom 
	END AS 
	geom
FROM ( 
SELECT analysis_id, street, direction, from_street, to_street, 
		gis.twochar_direction(gis.direction_from_line(ST_linemerge(ST_union(geom)))) AS geom_dir,
		ST_linemerge(ST_union(geom)) AS geom
FROM lines
GROUP BY analysis_id, street, direction, from_street, to_street) a)