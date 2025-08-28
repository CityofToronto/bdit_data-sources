WITH summary AS (
    --find the number of groups required to have no more than 100 per group
    SELECT
        FLOOR(COUNT(DISTINCT segment_id)
        / CEIL((COUNT(DISTINCT segment_id)) / 100.0)) AS num_per_group
    FROM congestion.network_segments_24_4
),

groups AS (
    SELECT
        CEIL(ROW_NUMBER() OVER (ORDER BY segment_id) / summary.num_per_group) AS group_id,
        segment_id
    FROM congestion.network_segments_24_4, summary
)

SELECT
    group_id,
    array_agg(segment_id),
    COUNT(*)
FROM groups
GROUP BY group_id
ORDER BY group_id