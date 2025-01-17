-- PROCEDURE: gwolofs.cache_tt_segment(text[])

-- DROP PROCEDURE IF EXISTS gwolofs.cache_tt_segment(text[]);

CREATE OR REPLACE PROCEDURE gwolofs.cache_tt_segment(
    IN link_dirs text[]
)
LANGUAGE 'sql'
AS $BODY$

INSERT INTO gwolofs.tt_segments (link_dirs, lengths, geom, total_length)
SELECT
    ARRAY_AGG(link_dir ORDER BY link_dir) AS link_dirs,
    ARRAY_AGG(length ORDER BY link_dir) AS lengths,
    st_union(st_linemerge(geom)) AS geom,
    SUM(length) AS total_length
FROM congestion.network_links_23_4_geom
WHERE link_dir = ANY (cache_tt_segment.link_dirs)
ON CONFLICT (link_dirs)
DO NOTHING;

$BODY$;

ALTER PROCEDURE gwolofs.cache_tt_segment(text[])
OWNER TO gwolofs;
