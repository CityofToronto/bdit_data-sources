--DROP FUNCTION here_agg.monthly_segment_vkt_agg;
CREATE OR REPLACE FUNCTION here_agg.monthly_segment_vkt_agg(
    p_mnth date,
    segments bigint [] DEFAULT NULL -- NULL = "all segments"
)
RETURNS TABLE (
    segment_id bigint,
    mnth date,
    highway boolean,
    sample_size numeric,
    vkt_km double precision,
    sqrt_vkt_km double precision
)
SECURITY DEFINER
LANGUAGE sql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

    SELECT
        cl.segment_id,
        monthly_segment_vkt_agg.p_mnth,
        cs.highway,
        SUM(vkt.sample_size) AS sample_size,
        SUM(vkt.sample_size::double precision * vkt.length) / 1000.0::double precision AS vkt_km,
        SUM(SQRT(vkt.sample_size::double precision) * vkt.length) / 1000.0::double precision AS sqrt_vkt_km
    FROM congestion.congestion_links AS cl
    JOIN congestion.congestion_segments AS cs USING (segment_id, ver_id)
    --no ver_id in this join, because when network changes, we need to use link_dir data from previous version as well.
    JOIN here_agg.monthly_link_sample_size AS vkt USING (link_dir)
    WHERE
        cl.ver_id = here_agg.select_map_version(
            start_date := monthly_segment_vkt_agg.p_mnth,
            end_date := (monthly_segment_vkt_agg.p_mnth + interval '1 month')::date,
            agg_type := 'path_hm'
        )
        --use 6 month rolling vkt to align with overnight speeds
        AND vkt.mnth >= monthly_segment_vkt_agg.p_mnth - interval '6 month'
        AND vkt.mnth < monthly_segment_vkt_agg.p_mnth
        AND (
            monthly_segment_vkt_agg.segments IS NULL
            OR cl.segment_id = ANY(monthly_segment_vkt_agg.segments)
        )
    GROUP BY
        vkt.mnth,
        cs.highway,
        cl.segment_id;

$BODY$;

ALTER FUNCTION here_agg.monthly_segment_vkt_agg(date, bigint [])
OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_agg.monthly_segment_vkt_agg TO congestion_bot;
REVOKE EXECUTE ON FUNCTION here_agg.monthly_segment_vkt_agg FROM public;

--SELECT * FROM here_agg.monthly_segment_vkt_agg('2025-01-01');
