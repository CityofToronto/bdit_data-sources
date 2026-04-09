-- FUNCTION: here_agg.area_tti_agg(date)

-- DROP FUNCTION IF EXISTS here_agg.area_tti_agg(date);

CREATE OR REPLACE FUNCTION here_agg.area_tti_agg(
    mnth date
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
    FROM here_agg.monthly_segment_vkt_agg(area_tti_agg.mnth) AS vkt
    JOIN here_agg.segment_areas AS area USING (segment_id) --this should have ver_id eventually
    ORDER BY segment_id;

    CREATE INDEX IF NOT EXISTS segment_list_idx
    ON segment_list (segment_id);

    INSERT INTO here_agg.area_tti (area_name, highway, dt, hr, is_wkdy, tti, num_segments)
    SELECT
        seg.area_name,
        seg.highway,
        overn.dt,
        hrly.hr,
        overn.is_wkdy, 
        SUM(hrly.avg_tt / overn.overnight_avg_tt * seg.vkt_km) / SUM(seg.vkt_km) AS tti,
        COUNT(DISTINCT hrly.segment_id) AS num_segments
    FROM segment_list AS seg
    LEFT JOIN here_agg.segment_overnight_tts AS overn
    ON
        seg.segment_id = overn.segment_id
        AND overn.dt >= area_tti_agg.mnth
        AND overn.dt < area_tti_agg.mnth + interval '1 month'
    JOIN here_agg.hourly_avg_tt AS hrly
        ON hrly.segment_id = overn.segment_id
        AND hrly.dt = overn.dt
        AND hrly.is_wkdy = overn.is_wkdy
    GROUP BY
        seg.area_name,
        seg.highway,
        overn.dt,
        hrly.hr,
        overn.is_wkdy
    ORDER BY
        seg.area_name,
        seg.highway,
        overn.is_wkdy,
        hrly.hr,
        overn.dt
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
