CREATE OR REPLACE FUNCTION here_agg.segment_grouping(
    date_start date,
    date_end date,
    max_group_size int default 100
)
RETURNS TABLE (
    segments text[]
) AS $$

WITH segments AS (
    --segments active in relevant month
    SELECT DISTINCT segment_id
    FROM here_agg.raw_segments
    WHERE
        dt >= date_start
        AND dt < date_end
),

group_size AS (
    --find the number of groups required to have no more than `max_group_size` per group
    SELECT
        FLOOR(
            COUNT(*)
            / CEIL((COUNT(*)) / max_group_size::numeric)
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

$$
LANGUAGE sql
SECURITY DEFINER;

ALTER FUNCTION here_agg.segment_grouping OWNER TO here_admins;
GRANT EXECUTE ON FUNCTION here_agg.segment_grouping TO here_bot;

--SELECT segments FROM here_agg.segment_grouping('2025-01-01', '2026-01-01')
