-- FUNCTION: here_agg.area_tti_agg(date)

-- DROP FUNCTION IF EXISTS here_agg.area_tti_agg(date);

CREATE OR REPLACE FUNCTION here_agg.area_tti_agg(
    dt date
)
RETURNS void
LANGUAGE plpgsql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
BEGIN

    DROP TABLE IF EXISTS segment_list;
    CREATE TEMP TABLE segment_list ON COMMIT DROP AS
    SELECT
        area_name,
        COALESCE(cs.highway, False) AS highway,
        pkt.segment_id,
        COALESCE(pkt.pkt_km, 0) AS pkt_km
    FROM here_agg.segment_6month_lookback AS pkt
    --gets the segment/area combos for the right map version, based on date
    JOIN here_agg.segment_areas(area_tti_agg.dt) AS area USING (segment_id)
    JOIN congestion.congestion_segments AS cs USING (segment_id, ver_id)
    WHERE pkt.mnth = date_trunc('month', area_tti_agg.dt)
    ORDER BY segment_id;

    CREATE INDEX IF NOT EXISTS segment_list_idx
    ON segment_list (segment_id);

    INSERT INTO here_agg.area_tti_segments (
        area_name, segment_id, highway, dt, hr, tti, pkt_km, weighted_tti
    )
    SELECT
        seg.area_name,
        seg.segment_id,
        seg.highway,
        area_tti_agg.dt AS dt,
        gs.hr,
        hrly.avg_tt / overn.overnight_avg_tt AS tti,
        seg.pkt_km,
        hrly.avg_tt / overn.overnight_avg_tt * seg.pkt_km AS weighted_tti
    FROM segment_list AS seg
    LEFT JOIN here_agg.segment_6month_lookback AS overn
    ON
        seg.segment_id = overn.segment_id
        AND overn.mnth = date_trunc('month', area_tti_agg.dt)
    --ensure missing segment-hours are retained
    CROSS JOIN generate_series(0, 23) AS gs(hr)
    LEFT JOIN here_agg.segment_travel_times_hrly_avg AS hrly
        ON seg.segment_id = hrly.segment_id
        AND hrly.hr = gs.hr
        AND hrly.dt = area_tti_agg.dt
    ON CONFLICT ON CONSTRAINT area_tti_segments_pkey
    DO UPDATE SET
        tti = EXCLUDED.tti,
        pkt_km = EXCLUDED.pkt_km,
        weighted_tti = EXCLUDED.weighted_tti;

    INSERT INTO here_agg.area_tti (
        area_name, dt, hr, road_category, tti, num_segments, num_segments_total
    )
    SELECT
        area_name,
        seg.dt,
        hr,
        CASE highway WHEN True THEN 'Highway' WHEN False THEN 'Non-Highway' ELSE 'All' END AS road_category,
        SUM(tti * pkt_km) / SUM(pkt_km) AS tti,
        COUNT(*) FILTER (WHERE weighted_tti IS NOT NULL) AS num_segments,
        COUNT(*) AS num_segments_total
    FROM here_agg.area_tti_segments AS seg
    WHERE seg.dt = area_tti_agg.dt
    GROUP BY
        area_name,
        seg.dt,
        hr,
        ROLLUP(highway) --To get highway: T/F/All
    ORDER BY
        area_name,
        highway,
        hr,
        seg.dt
    ON CONFLICT ON CONSTRAINT area_tti_pkey
    DO UPDATE SET
        tti = EXCLUDED.tti,
        num_segments = EXCLUDED.num_segments,
        num_segments_total = EXCLUDED.num_segments_total;

END;
$BODY$;

ALTER FUNCTION here_agg.area_tti_agg(date)
OWNER TO here_admins;

REVOKE EXECUTE ON FUNCTION here_agg.area_tti_agg(date) FROM public;

GRANT EXECUTE ON FUNCTION here_agg.area_tti_agg(date) TO congestion_bot;

GRANT EXECUTE ON FUNCTION here_agg.area_tti_agg(date) TO here_admins;

--SELECT here_agg.area_tti_agg('2024-07-01')
