-- FUNCTION: here_agg.monthly_link_vkt_agg(date)

-- DROP FUNCTION IF EXISTS here_agg.monthly_link_vkt_agg(date);

CREATE OR REPLACE FUNCTION here_agg.monthly_link_vkt_agg(
    mnth date
)
RETURNS void
SECURITY DEFINER
LANGUAGE plpgsql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

DECLARE
    map_version text := here_agg.select_map_version(
        start_date := monthly_link_vkt_agg.mnth,
        end_date := (monthly_link_vkt_agg.mnth + interval '1 month')::date,
        agg_type := 'path_hm'
    );
    links_table text := 'congestion_links_' || map_version;

BEGIN

    EXECUTE format($sql$
        INSERT INTO here_agg.monthly_link_vkt (mnth, link_dir, ver_id, length, sample_size)
        SELECT
            date_trunc('month', ta_path.tx) AS mnth,
            links.link_dir,
            %3$L AS ver_id,
            links.length,
            SUM(ta_path.sample_size) AS sample_size
        FROM here.ta_path_hm AS ta_path
        JOIN congestion.%1$I AS links USING (link_dir)
        WHERE
            ta_path.tx >= %2$L::date
            AND ta_path.tx < %2$L::date + interval '1 month'
        GROUP BY
            mnth,
            links.link_dir,
            links.length
        ON CONFLICT ON CONSTRAINT monthly_link_vkt_pkey
        DO UPDATE SET
            length = EXCLUDED.length,
            sample_size = EXCLUDED.sample_size;
    $sql$, links_table, mnth, map_version);
    
END;
$BODY$;

ALTER FUNCTION here_agg.monthly_link_vkt_agg(date)
OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_agg.monthly_link_vkt_agg TO congestion_bot;
