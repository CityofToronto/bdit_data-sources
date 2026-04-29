CREATE INDEX IF NOT EXISTS network_segments_24_4_geom_idx
ON congestion.network_segments_24_4
USING gist (geom);

CREATE VIEW here_agg.segment_areas AS

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
    FROM congestion.network_segments_24_4 AS seg
    JOIN gis.community_council_2018 AS cc
        ON cc.the_geom && seg.geom --quick bounding box intersection using gist idx
    --WHERE cc.area_name = 'North York Community Council'
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
FROM congestion.network_segments_24_4 AS seg

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
FROM congestion.network_segments_24_4 AS seg
JOIN gis.to_core_downtown AS dt
    ON st_intersects(st_transform(dt.geom, 4326), seg.geom);