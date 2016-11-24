
SELECT gid, tmc,
	CASE WHEN (direction = 'Eastbound' AND ST_X(ST_StartPoint(ST_LineMerge(geom))) > ST_X(ST_EndPoint(ST_LineMerge(geom))) )
	OR (direction = 'Westbound' AND ST_X(ST_StartPoint(ST_LineMerge(geom))) < ST_X(ST_EndPoint(ST_LineMerge(geom))) )
	OR (direction = 'Northbound' AND ST_Y(ST_StartPoint(ST_LineMerge(geom))) > ST_Y(ST_EndPoint(ST_LineMerge(geom))) )
	OR (direction = 'Southbound' AND ST_Y(ST_StartPoint(ST_LineMerge(geom))) < ST_Y(ST_EndPoint(ST_LineMerge(geom))) )
	THEN ST_REVERSE(geom)::geometry(MultiLineString,4326) 
	ELSE geom::geometry(MultiLineString,4326)
	END AS geom,
	direction,
	direction_from_line(geom)
  INTO gis.inrix_tmc_tor_flipped
  FROM gis.inrix_tmc_tor
  WHERE road_type IS NULL;
  COMMENT ON TABLE gis.inrix_tmc_tor_flipped IS 'Correcting drawn direction of arterial TMCs';
