--DROP FUNCTION here_agg.monthly_segment_vkt_agg;
CREATE OR REPLACE FUNCTION here_agg.monthly_segment_vkt_agg(
    p_mnth date
)
RETURNS TABLE (
    segment_id bigint,
    mnth date,
    ver_id text,
    highway boolean,
    sample_size numeric,
    vkt_km double precision,
    sqrt_vkt_km double precision
)
SECURITY DEFINER
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

DECLARE
    map_version text := here_agg.select_map_version(
        start_date := monthly_segment_vkt_agg.p_mnth,
        end_date := (monthly_segment_vkt_agg.p_mnth + interval '1 month')::date,
        agg_type := 'path_hm'
    );
    links_table text := 'network_links_' || map_version;

BEGIN

    RETURN QUERY EXECUTE format($sql$
        SELECT
            nl.segment_id,
            %2$L::date,
            vkt.ver_id,
            cs.highway,
            SUM(vkt.sample_size) AS sample_size,
            SUM(vkt.sample_size::double precision * vkt.length) / 1000.0::double precision AS vkt_km,
            SUM(SQRT(vkt.sample_size::double precision) * vkt.length) / 1000.0::double precision AS sqrt_vkt_km
        FROM congestion.%1$I AS nl
        JOIN here_agg.monthly_link_vkt AS vkt USING (link_dir)
        JOIN congestion.congestion_segments AS cs USING (segment_id, ver_id)
        WHERE vkt.mnth = %2$L::date - interval '1 month' --use the VKT for previous month (enable more frequent publishing)
        GROUP BY
            vkt.mnth,
            vkt.ver_id,
            cs.highway,
            nl.segment_id;
    $sql$, links_table, p_mnth);

END;
$BODY$;

ALTER FUNCTION here_agg.monthly_segment_vkt_agg(date)
OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_agg.monthly_segment_vkt_agg TO congestion_bot;
REVOKE EXECUTE ON FUNCTION here_agg.monthly_segment_vkt_agg FROM public;

--SELECT * FROM here_agg.monthly_segment_vkt_agg('2025-01-01');
