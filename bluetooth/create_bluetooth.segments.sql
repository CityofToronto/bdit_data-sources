--new segments
DROP TABLE bluetooth.segments;
CREATE TABLE bluetooth.segments as (
SELECT routes.startpoint || '_' || routes.endpoint as segment_name, -- Ron's tables
	routes.analysis_id,
	pairs.street,
	seg.direction,
	pairs.from_street as start_crossstreet,
	pairs.to_street as end_crossstreet,
	seg.length_m as length,
	TRUE as bluetooth,
	FALSE as wifi,
	routes.geom
	
FROM ryu4.bt_route_pairs as pairs
	INNER JOIN ryu4.bt_segments seg ON (pairs.nb_eb_report_name = seg.report_name OR pairs.nb_eb_report_name = seg.report_name)
	INNER JOIN ryu4.bluetooth_routes routes USING (analysis_id)

UNION(

SELECT seg.segment_name, -- Original tables
	seg.analysis_id,
	CASE WHEN seg.start_road <> seg.end_road THEN seg.start_road || '/' || seg.end_road ELSE seg.start_road END as street,
	seg.direction,
	seg.start_crossstreet,
	seg.end_crossstreet,
	routes.length_m,
	TRUE as bluetooth,
	TRUE as wifi,
	routes.geom
		
FROM gis.bluetooth_routes as routes
	INNER JOIN bluetooth.ref_segments seg ON (seg.segment_name = routes.resultid)))