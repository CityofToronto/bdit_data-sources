CREATE OR REPLACE FUNCTION here_agg.segment_grouping(
    date_start date,
    max_group_size int DEFAULT 100
)
RETURNS TABLE (
    segments text []
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $BODY$

DECLARE
    map_version text := here_agg.select_map_version(date_start, date_start + 1, 'path_hm');
    congestion_network_table text := CASE map_version
    WHEN '25_1' THEN 'congestion_segments_25_1'
    WHEN '24_4' THEN 'congestion_segments_24_4'
    WHEN '23_4' THEN 'network_segments_23_4_geom' END;

BEGIN

RETURN QUERY EXECUTE FORMAT($sql$
WITH segments AS (
    --segments active in relevant month
    SELECT DISTINCT segment_id
    FROM congestion.%1$I
),

group_size AS (
    --find the number of groups required to have no more than `max_group_size` per group
    SELECT
        FLOOR(
            COUNT(*)
            / CEIL((COUNT(*)) / %2$L::numeric)
        ) AS num_per_group
    FROM segments
),

groups AS (
    SELECT
        --assign group_ids using row number
        CEIL(ROW_NUMBER() OVER (ORDER BY segment_id) / group_size.num_per_group) AS group_id,
        segment_id
    FROM segments, group_size
),

groups_summarized AS (
    SELECT
        group_id,
        array_agg(segment_id) AS segment_ids,
        COUNT(*)
    FROM groups
    GROUP BY group_id
    ORDER BY group_id
)

--return list of lists for xcom
SELECT array_agg(segment_ids::text)
FROM groups_summarized;

$sql$, congestion_network_table, max_group_size);

END;
$BODY$;

ALTER FUNCTION here_agg.segment_grouping OWNER TO here_admins;
GRANT EXECUTE ON FUNCTION here_agg.segment_grouping TO here_bot;

--SELECT segments FROM here_agg.segment_grouping('2026-01-01')