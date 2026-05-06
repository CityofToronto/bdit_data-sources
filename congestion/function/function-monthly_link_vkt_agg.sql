-- FUNCTION: here_agg.monthly_segment_vkt_agg(date, bigint [])

-- DROP FUNCTION IF EXISTS here_agg.monthly_segment_vkt_agg(date, bigint []);

CREATE OR REPLACE FUNCTION here_agg.monthly_segment_vkt_agg(
    mnth date,
    segments bigint [] DEFAULT NULL -- NULL = "all segments"
)
RETURNS void
SECURITY DEFINER
LANGUAGE sql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

    INSERT INTO here_agg.monthly_segment_vkt (mnth, segment_id, ver_id, highway, sample_size, vkt_km, sqrt_vkt_km)
    SELECT
        monthly_segment_vkt_agg.mnth AS mnth,
        cl.segment_id,
        cl.ver_id,
        cs.highway,
        SUM(ta.sample_size) AS sample_size,
        SUM(ta.sample_size::double precision * cl.length) / 1000.0::double precision AS vkt_km,
        SUM(SQRT(ta.sample_size::double precision) * cl.length) / 1000.0::double precision AS sqrt_vkt_km
    FROM congestion.congestion_links AS cl
    JOIN congestion.congestion_segments AS cs USING (segment_id, ver_id)
    JOIN here.ta_path_hm AS ta USING (link_dir)
    WHERE
        cl.ver_id = here_agg.select_map_version(
            start_date := monthly_segment_vkt_agg.mnth,
            end_date := (monthly_segment_vkt_agg.mnth + interval '1 month')::date,
            agg_type := 'path_hm'
        )
        --should be mostly fine to query segment links accross multiple map versions
        --eg. if a segment gets split into two, most of the links would have existed before/after split
        AND ta.tx >= monthly_segment_vkt_agg.mnth::date - interval '6 months'
        AND ta.tx < monthly_segment_vkt_agg.mnth::date
        AND (
            monthly_segment_vkt_agg.segments IS NULL
            OR cl.segment_id = ANY(monthly_segment_vkt_agg.segments)
        )
    GROUP BY
        cs.highway,
        cl.ver_id,
        cl.segment_id
    ON CONFLICT ON CONSTRAINT monthly_segment_vkt_pkey
    DO UPDATE SET
        sample_size = EXCLUDED.sample_size,
        vkt_km = EXCLUDED.vkt_km,
        sqrt_vkt_km = EXCLUDED.sqrt_vkt_km;
    
$BODY$;

ALTER FUNCTION here_agg.monthly_segment_vkt_agg(date, bigint [])
OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_agg.monthly_segment_vkt_agg TO congestion_bot;

--SELECT here_agg.monthly_segment_vkt_agg('2026-01-01')
