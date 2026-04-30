--DROP FUNCTION here_agg.monthly_segment_vkt_agg;
CREATE OR REPLACE FUNCTION here_agg.monthly_segment_vkt_agg(
    p_mnth date
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
LANGUAGE plpgsql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

DECLARE
    map_version text := here_agg.select_map_version(
        start_date := monthly_segment_vkt_agg.p_mnth,
        end_date := (monthly_segment_vkt_agg.p_mnth + interval '1 month')::date,
        agg_type := 'path_hm'
    );

BEGIN

    RETURN QUERY EXECUTE format($sql$
        SELECT
            cl.segment_id,
            %2$L::date,
            cs.highway,
            SUM(vkt.sample_size) AS sample_size,
            SUM(vkt.sample_size::double precision * vkt.length) / 1000.0::double precision AS vkt_km,
            SUM(SQRT(vkt.sample_size::double precision) * vkt.length) / 1000.0::double precision AS sqrt_vkt_km
        FROM congestion.congestion_links AS cl
        JOIN congestion.congestion_segments AS cs USING (segment_id)
        --no ver_id in this join, because when network changes, we need to use link_dir data from previous version as well.
        JOIN here_agg.monthly_link_vkt AS vkt USING (link_dir)
        WHERE
            cl.ver_id = %1$L
            AND cs.ver_id = %1$L
            --use 6 month rolling vkt to align with overnight speeds
            AND vkt.mnth >= %2$L::date - interval '6 month'
            AND vkt.mnth < %2$L::date
        GROUP BY
            vkt.mnth,
            cs.highway,
            cl.segment_id;
    $sql$, map_version, p_mnth);

END;
$BODY$;

ALTER FUNCTION here_agg.monthly_segment_vkt_agg(date)
OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_agg.monthly_segment_vkt_agg TO congestion_bot;
REVOKE EXECUTE ON FUNCTION here_agg.monthly_segment_vkt_agg FROM public;

--SELECT * FROM here_agg.monthly_segment_vkt_agg('2025-01-01');
