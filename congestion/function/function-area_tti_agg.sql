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
    SELECT area_name, vkt.highway, vkt.segment_id, vkt.vkt_km
    FROM here_agg.monthly_segment_vkt_agg(date_trunc('month', area_tti_agg.dt)) AS vkt
    JOIN here_agg.segment_areas AS area USING (segment_id) --this should have ver_id eventually
    ORDER BY segment_id;

    CREATE INDEX IF NOT EXISTS segment_list_idx
    ON segment_list (segment_id);

    INSERT INTO here_agg.area_tti (area_name, road_category, dt, hr, tti, num_segments)
    SELECT
        seg.area_name,
        CASE seg.highway WHEN True THEN 'Highway' WHEN False THEN 'Non-Highway' ELSE 'All' END AS road_category,
        hrly.dt,
        hrly.hr,
        SUM(hrly.avg_tt / overn.overnight_avg_tt * seg.vkt_km) / SUM(seg.vkt_km) AS tti,
        COUNT(DISTINCT hrly.segment_id) AS num_segments
    FROM segment_list AS seg
    LEFT JOIN here_agg.segment_overnight_tts AS overn
    ON
        seg.segment_id = overn.segment_id
        AND overn.mnth = date_trunc('month', area_tti_agg.dt)
    JOIN here_agg.hourly_avg_tt AS hrly
        ON hrly.segment_id = overn.segment_id
        AND hrly.dt >= overn.mnth
        AND hrly.dt < overn.mnth + interval '1 month'
    GROUP BY
        seg.area_name,
        ROLLUP(seg.highway), --To get highway: T/F/All
        overn.mnth,
        hrly.hr,
        hrly.dt
    ORDER BY
        seg.area_name,
        seg.highway,
        hrly.hr,
        hrly.dt
    ON CONFLICT ON CONSTRAINT area_tti_pkey
    DO UPDATE SET
        tti = EXCLUDED.tti,
        num_segments = EXCLUDED.num_segments;

END;
$BODY$;

ALTER FUNCTION here_agg.area_tti_agg(date)
OWNER TO here_admins;

REVOKE EXECUTE ON FUNCTION here_agg.area_tti_agg(date) FROM public;

GRANT EXECUTE ON FUNCTION here_agg.area_tti_agg(date) TO congestion_bot;

GRANT EXECUTE ON FUNCTION here_agg.area_tti_agg(date) TO here_admins;

--SELECT here_agg.area_tti_agg('2024-07-01')
