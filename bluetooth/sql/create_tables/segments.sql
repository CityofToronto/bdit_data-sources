--new segments
DROP TABLE bluetooth.segments;
CREATE TABLE bluetooth.segments AS (
    SELECT
        routes.startpoint || '_' || routes.endpoint AS segment_name, -- Ron's tables
        routes.analysis_id,
        pairs.street,
        seg.direction,
        pairs.from_street AS start_crossstreet,
        pairs.to_street AS end_crossstreet,
        seg.length_m AS length,
        TRUE AS bluetooth,
        FALSE AS wifi,
        routes.geom

    FROM ryu4.bt_route_pairs AS pairs
    INNER JOIN
        ryu4.bt_segments AS seg
        ON (pairs.nb_eb_report_name = seg.report_name OR pairs.nb_eb_report_name = seg.report_name)
    INNER JOIN ryu4.bluetooth_routes AS routes USING (analysis_id)

    UNION
    (

        SELECT
            seg.segment_name, -- Original tables
            seg.analysis_id,
            seg.direction,
            seg.start_crossstreet,
            seg.end_crossstreet,
            routes.length_m,
            TRUE AS bluetooth,
            TRUE AS wifi,
            routes.geom,
            CASE
                WHEN
                    seg.start_road != seg.end_road
                    THEN seg.start_road || '/' || seg.end_road
                ELSE seg.start_road
            END AS street

        FROM gis.bluetooth_routes AS routes
        INNER JOIN bluetooth.ref_segments AS seg ON (seg.segment_name = routes.resultid)
    )
)
