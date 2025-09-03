WITH segments AS (
    --segments active in relevant month
    SELECT DISTINCT segment_id
    FROM gwolofs.congestion_raw_segments
    WHERE
        dt >= {{ ds }}::date
        AND dt < {{ ds }}::date + '1 month'::interval 
),

group_size AS (
    --find the number of groups required to have no more than `max_group_size` per group
    SELECT
        FLOOR(COUNT(*)
        / CEIL((COUNT(*)) / {{ params.max_group_size }}::numeric)) AS num_per_group
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
FROM groups_summarized