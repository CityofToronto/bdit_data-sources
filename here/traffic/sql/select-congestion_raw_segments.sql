--TRUNCATE gwolofs.congestion_raw_segments;

--INSERT 0 771478
--Query returned successfully in 2 min 36 secs.
-- vs 7,756,256 rows in (SELECT COUNT(*) FROM here.ta_path WHERE dt = '2025-01-04') = 1/10

WITH segment_5min_bins AS (
    SELECT
        segments.segment_id,
        date_trunc('hour', ta.tx) AS time_grp,
        ta.tx,
        RANK() OVER w AS bin_rank,
        SUM(links.length) / segments.total_length AS sum_length,
        SUM(sample_size) AS num_obs,
        ARRAY_AGG(ta.link_dir ORDER BY link_dir) AS link_dirs,
        ARRAY_AGG(links.length / ta.mean * 3.6 ORDER BY link_dir) AS tts,
        ARRAY_AGG(links.length ORDER BY link_dir) AS lengths
    FROM here.ta_path AS ta
    JOIN congestion.network_links_23_4_geom AS links USING (link_dir)
    JOIN congestion.network_segments_23_4_geom AS segments USING (segment_id)
    WHERE ta.dt = '2025-01-04'
        --AND tx < '2025-01-04 01:00:00'
        --AND segment_id = 1 AND date_trunc('hour', ta.tx) = '2025-01-04 00:00:00'
    GROUP BY
        segments.segment_id,
        ta.tx,
        segments.total_length
   WINDOW w AS (
        PARTITION BY segments.segment_id, date_trunc('hour', ta.tx)
        ORDER BY ta.tx
   )
),

dynamic_bin_options AS (
    --within each segment/hour, generate all possible forward looking bin combinations
    --don't generate options for bins with sufficient length
    --also don't generate options past the next bin with 80% length
    SELECT
        tx,
        time_grp,
        segment_id,
        bin_rank AS start_bin,
        --generate all the options for the end bin within the group.
        generate_series(
            CASE
                WHEN sum_length >= 0.8 THEN bin_rank
                --if length is insufficient, need at least 1 more bin
                ELSE LEAST(bin_rank + 1, MAX(bin_rank) OVER w)
            END,
            CASE
                --dont need to generate options when start segment is already sufficient
                WHEN sum_length >= 0.8 THEN bin_rank
                --generate options until 1 bin has sufficient length, otherwise until last bin in group
                ELSE COALESCE(MIN(bin_rank) FILTER (WHERE sum_length >= 0.8) OVER w, MAX(bin_rank) OVER w)
            END,
            1
        ) AS end_bin
    FROM segment_5min_bins
    WINDOW w AS (
        PARTITION BY time_grp, segment_id
        ORDER BY tx
        --look only forward for end_bin options
        RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    )
),

unnested_db_options AS (
    SELECT
        dbo.time_grp,
        dbo.segment_id,
        dbo.tx AS dt_start,
        --exclusive end bin
        MAX(s5b.tx) + interval '5 minutes' AS dt_end,
        unnested.link_dir,
        unnested.len,
        AVG(unnested.tt) AS tt, --avg TT for each link_dir
        SUM(num_obs) AS num_obs --sum of here.ta_path sample_size for each link_dir
    FROM dynamic_bin_options AS dbo
    LEFT JOIN segment_5min_bins AS s5b
        ON s5b.time_grp = dbo.time_grp
        AND s5b.segment_id = dbo.segment_id
        AND s5b.bin_rank >= dbo.start_bin
        AND s5b.bin_rank <= dbo.end_bin,
    --unnest all the observations from individual link_dirs to reaggregate them within new dynamic bin
    UNNEST(s5b.link_dirs, s5b.lengths, s5b.tts) AS unnested(link_dir, len, tt)
    GROUP BY
        dbo.time_grp,
        dbo.segment_id,
        dbo.tx,
        dbo.end_bin,
        unnested.link_dir,
        unnested.len
)

INSERT INTO gwolofs.congestion_raw_segments (
    time_grp, segment_id, dt_start, dt_end, bin_range, tt,
    unadjusted_tt, total_length, length_w_data, num_obs
)
--this query contains overlapping values which get eliminated
--via on conflict with the exclusion constraint on congestion_raw_segments table.
SELECT DISTINCT ON (udbo.time_grp, udbo.segment_id, udbo.dt_start)
    udbo.time_grp,
    udbo.segment_id,
    udbo.dt_start,
    udbo.dt_end,
    tsrange(udbo.dt_start, udbo.dt_end, '[)') AS bin_range,
    segments.total_length / SUM(udbo.len) * SUM(udbo.tt) AS tt,
    SUM(udbo.tt) AS unadjusted_tt,
    segments.total_length,
    SUM(udbo.len) AS length_w_data,
    SUM(udbo.num_obs) AS num_obs --sum of here.ta_path sample_size for each segment
FROM unnested_db_options AS udbo
LEFT JOIN congestion.network_segments_23_4_geom AS segments USING (segment_id)
GROUP BY
    udbo.time_grp,
    udbo.segment_id,
    udbo.dt_start,
    udbo.dt_end,
    segments.total_length
HAVING SUM(udbo.len) >= 0.8 * segments.total_length
ORDER BY
    udbo.time_grp,
    udbo.segment_id,
    udbo.dt_start,
    udbo.dt_end
--exclusion constraint + ordered insert to prevent overlapping bins
ON CONFLICT ON CONSTRAINT congestion_raw_segments_unique
DO NOTHING;
    
/*
--bins which were not used. Might consider adding these on to bins that already have sufficient data.
SELECT *
FROM gwolofs.segment_5min_bins AS s5b
LEFT JOIN gwolofs.congestion_raw_segments AS dyb ON
    s5b.time_grp = dyb.time_grp
    AND s5b.source_node = dyb.source_node
    AND s5b.dest_node = dyb.dest_node
    AND s5b.tx <@ dyb.bin_range    
WHERE dyb.bin_range IS NULL 
*/

/*
WITH hourly_obs AS (
    SELECT time_grp, segment_id, AVG(tt) AS avg_hour_tt, COUNT(*)
    FROM gwolofs.congestion_raw_segments
    GROUP BY time_grp, segment_id
)

SELECT segment_id, date_part('hour', time_grp), AVG(avg_hour_tt) AS avg_tt, SUM(count)
FROM hourly_obs
GROUP BY 1, 2 ORDER BY 1, 2;
*/