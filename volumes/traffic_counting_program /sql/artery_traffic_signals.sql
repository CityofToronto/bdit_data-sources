CREATE VIEW traffic.artery_traffic_signals AS

    SELECT 			a.arterycode,
					a.px,
					a.location

    FROM 			traffic.artery_locations_px a
    INNER JOIN 		vz_safety_programs_staging.signals_cart USING (px)
    WHERE 			asset_type = 'Traffic Signals';
	
COMMENT ON VIEW traffic.artery_traffic_signals IS 'Lookup between artery codes and px numbers that have traffic signals';
GRANT SELECT ON TABLE traffic.artery_traffic_signals TO bdit_humans;
