-- DROP FUNCTION here_agg.segment_areas;

CREATE OR REPLACE FUNCTION here_agg.segment_areas(
    dt date
)
RETURNS TABLE (
    segment_id bigint,
    --geom geometry,
    total_length double precision,
    highway	boolean,
    --dir	text,
    area_name character varying
) AS $$

DECLARE
    map_verison text := here_agg.select_map_version(
        start_date := segment_areas.dt,
        end_date := segment_areas.dt + 1,
        agg_type := 'path_hm'
    );

BEGIN

RETURN QUERY
WITH community_councils AS (
    SELECT DISTINCT ON (seg.segment_id)
        seg.segment_id,
        --seg.geom,
        seg.total_length,
        seg.highway,
        --seg.dir,
        cc.area_name
    FROM congestion.congestion_segments AS seg
    JOIN gis.community_council_2018 AS cc
        ON cc.the_geom && seg.geom --quick bounding box intersection using gist idx
    WHERE
        seg.ver_id = map_verison
        AND st_intersects(cc.the_geom, seg.geom)
    ORDER BY
        seg.segment_id,
        --combined with distinct on, takes the community council with the highest overlap
        ST_length(ST_Intersection(cc.the_geom, seg.geom)) DESC
)

SELECT
    seg.segment_id,
    --seg.geom,
    seg.total_length,
    seg.highway,
    --seg.dir,
    seg.area_name
FROM community_councils AS seg

UNION

SELECT
    seg.segment_id,
    --seg.geom,
    seg.total_length,
    seg.highway,
    --seg.dir,
    'Citywide' AS area_name
FROM congestion.congestion_segments AS seg
WHERE seg.ver_id = map_verison

UNION

SELECT
    seg.segment_id,
    --seg.geom,
    seg.total_length,
    seg.highway,
    --seg.dir,
    'Downtown' AS area_name
FROM congestion.congestion_segments AS seg
JOIN gis.to_core_downtown AS dt
    ON st_intersects(st_transform(dt.geom, 4326), seg.geom)
    --at least 50% length overlap
    AND ST_Length(ST_Intersection(seg.geom, st_transform(dt.geom, 4326))) / ST_Length(seg.geom) >= 0.50
WHERE seg.ver_id = map_verison;

END;

$$ LANGUAGE plpgsql;

ALTER FUNCTION here_agg.segment_areas OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_agg.segment_areas TO congestion_bot;

COMMENT ON FUNCTION here_agg.segment_areas
IS 'Return the segments which make up each TTI Area group for a specific date.';

--test
--SELECT * FROM here_agg.segment_areas('2026-06-02')
