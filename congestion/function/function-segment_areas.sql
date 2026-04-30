CREATE OR REPLACE FUNCTION here_agg.segment_areas(
    mnth date
)
RETURNS TABLE (
    segment_id bigint,
    start_vid bigint,
    end_vid bigint,
    geom geometry,
    total_length double precision,
    highway	boolean,
    dir	character varying,
    area_name character varying
) AS $$

WITH community_councils AS (
    SELECT DISTINCT ON (seg.segment_id)
        seg.segment_id,
        seg.start_vid,
        seg.end_vid,
        seg.geom,
        seg.total_length,
        seg.highway,
        seg.dir,
        cc.area_name
    FROM congestion.congestion_segments AS seg
    JOIN gis.community_council_2018 AS cc
        ON cc.the_geom && seg.geom --quick bounding box intersection using gist idx
    --WHERE cc.area_name = 'North York Community Council'
    WHERE seg.ver_id = here_agg.select_map_version(
        start_date := segment_areas.mnth,
        end_date := segment_areas.mnth + 1,
        agg_type := 'path_hm'
    )
    ORDER BY
        seg.segment_id,
        ST_length(ST_Intersection(cc.the_geom, seg.geom)) DESC
)

SELECT
    segment_id,
    start_vid,
    end_vid,
    geom,
    total_length,
    highway,
    dir,
    area_name
FROM community_councils

UNION

SELECT
    seg.segment_id,
    seg.start_vid,
    seg.end_vid,
    seg.geom,
    seg.total_length,
    seg.highway,
    seg.dir,
    'Citywide' AS area_name
FROM congestion.congestion_segments AS seg
WHERE seg.ver_id = here_agg.select_map_version(
    start_date := segment_areas.mnth,
    end_date := segment_areas.mnth + 1,
    agg_type := 'path_hm'
)

UNION

SELECT
    seg.segment_id,
    seg.start_vid,
    seg.end_vid,
    seg.geom,
    seg.total_length,
    seg.highway,
    seg.dir,
    'Downtown' AS area_name
FROM congestion.congestion_segments AS seg
JOIN gis.to_core_downtown AS dt
    ON st_intersects(st_transform(dt.geom, 4326), seg.geom)
WHERE seg.ver_id = here_agg.select_map_version(
    start_date := segment_areas.mnth,
    end_date := segment_areas.mnth + 1,
    agg_type := 'path_hm'
);

$$ LANGUAGE sql;

ALTER FUNCTION here_agg.segment_areas OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_agg.segment_areas TO congestion_bot;
