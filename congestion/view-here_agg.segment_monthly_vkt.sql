--DROP VIEW here_agg.segment_monthly_vkt;

CREATE OR REPLACE VIEW here_agg.segment_monthly_vkt AS
SELECT
    a.mnth,
    a.ver_id,
    nl.segment_id,
    congestion_centreline.centreline_ids,
    a.length,
    SUM(a.sample_size) AS sample_size,
    -- Total VKT per segment in kilometers (volume-based)
    SUM(a.sample_size * a.length) / 1000.0 AS vkt_km,
    -- Total VKT per segment in kilometers (square root of volume as weight)
    SUM(SQRT(a.sample_size) * a.length) / 1000.0 AS sqrt_vkt_km
FROM congestion.network_links_24_4 AS nl
JOIN here_agg.monthly_link_vkt AS a USING (link_dir)
JOIN congestion.congestion_centreline USING (segment_id, ver_id)
GROUP BY
    a.mnth,
    a.ver_id,
    congestion_centreline.centreline_ids,
    a.length,
    nl.segment_id;
