CREATE VIEW traffic.artery_locations_px AS (


    SELECT
        arterydata.arterycode,
        traffic_signal.px,
        arterydata.location,
        signals.geom
    FROM traffic.arterydata
    JOIN traffic.arteries_centreline USING (arterycode)
    JOIN traffic.traffic_signal
        ON arteries_centreline.centreline_id = traffic_signal."centrelineId"
    JOIN gis.traffic_signal AS signals
        ON trim(LEADING '0' FROM signals.px)::int = traffic_signal.px
    ORDER BY traffic_signal.px

);

COMMENT ON VIEW traffic.artery_locations_px IS
'Lookup between artery codes and px numbers (intersections). '
'Note that multiple arterycodes can be mapped to a single PX. ';

GRANT SELECT ON TABLE traffic.artery_locations_px TO bdit_humans;
