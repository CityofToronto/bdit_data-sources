-- FUNCTION: here_agg.monthly_link_vkt_agg(date, bigint [])

-- DROP FUNCTION IF EXISTS here_agg.monthly_link_vkt_agg(date, bigint []);

CREATE OR REPLACE FUNCTION here_agg.monthly_link_vkt_agg(
    mnth date,
    segments bigint [] DEFAULT NULL -- NULL = "all segments"
)
RETURNS void
SECURITY DEFINER
LANGUAGE sql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

    INSERT INTO here_agg.monthly_link_vkt (mnth, link_dir, ver_id, length, sample_size)
    SELECT
        date_trunc('month', ta_path.tx) AS mnth,
        links.link_dir,
        links.ver_id,
        links.length,
        SUM(ta_path.sample_size) AS sample_size
    FROM here.ta_path_hm AS ta_path
    JOIN congestion.congestion_links AS links USING (link_dir)
    WHERE
        ta_path.tx >= monthly_link_vkt_agg.mnth::date
        AND ta_path.tx < monthly_link_vkt_agg.mnth::date + interval '1 month'
        AND links.ver_id = here_agg.select_map_version(
            start_date := monthly_link_vkt_agg.mnth,
            end_date := (monthly_link_vkt_agg.mnth + interval '1 month')::date,
            agg_type := 'path_hm'
        )
        AND (
            monthly_link_vkt_agg.segments IS NULL
            OR links.segment_id = ANY(monthly_link_vkt_agg.segments)
        )
    GROUP BY
        mnth,
        links.ver_id,
        links.link_dir,
        links.length
    ON CONFLICT ON CONSTRAINT monthly_link_vkt_pkey
    DO UPDATE SET
        length = EXCLUDED.length,
        sample_size = EXCLUDED.sample_size;
    
$BODY$;

ALTER FUNCTION here_agg.monthly_link_vkt_agg(date, bigint [])
OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_agg.monthly_link_vkt_agg TO congestion_bot;

--SELECT here_agg.monthly_link_vkt_agg('2026-01-01')
