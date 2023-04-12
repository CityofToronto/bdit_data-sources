CREATE VIEW traffic.artery_locations_px AS

	SELECT      arterycode,
                (regexp_matches(location::text, 'PX (\d+)'::text))[1]::integer AS px,
                regexp_replace(location::text, E'[\\n\\r]+', ' ', 'g') as location
    FROM        traffic.arterydata;
	
COMMENT ON VIEW traffic.artery_locations_px IS 'Lookup between artery codes and px numbers (intersections)';
GRANT SELECT ON TABLE traffic.artery_locations_px TO bdit_humans;
