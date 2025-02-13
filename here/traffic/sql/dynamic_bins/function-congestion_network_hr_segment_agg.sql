-- FUNCTION: gwolofs.congestion_day_hr_segment_agg(date)

-- DROP FUNCTION IF EXISTS gwolofs.congestion_day_hr_segment_agg(date);

CREATE OR REPLACE FUNCTION gwolofs.congestion_day_hr_segment_agg(
    start_date date)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

DECLARE
    map_version text := gwolofs.congestion_select_map_version(start_date, start_date + 1);
    congestion_network_table text := 'network_links_' || map_version;

BEGIN

EXECUTE FORMAT(
    $$
    WITH time_bins AS (
        SELECT
            start_time,
            start_time + '1 hour'::interval AS end_time,
            timerange(
                start_time::time,
                CASE start_time::time WHEN '23:00' THEN '24:00' ELSE start_time::time + '1 hour'::interval END,
                '[)') AS time_grp
        FROM generate_series(
            %1$L::date + '00:00'::time,
            %1$L::date + '23 hour'::interval, '1 hour'::interval) AS hours(start_time)
    ),

    segments AS (
        SELECT
            segment_id,
            link_dir,
            length,
            SUM(length) OVER (PARTITION BY segment_id) AS total_length
        FROM congestion.%2$I
    ),
    
    segment_5min_bins AS (
        SELECT
            links.segment_id,
            tb.time_grp,
            ta.tx,
            RANK() OVER w AS bin_rank,
            links.total_length,
            SUM(links.length) / links.total_length AS sum_length,
            SUM(links.length) AS length_w_data,
            SUM(links.length / ta.mean * 3.6) AS unadjusted_tt,
            SUM(sample_size) AS num_obs,
            ARRAY_AGG(ta.link_dir ORDER BY link_dir) AS link_dirs,
            ARRAY_AGG(links.length / ta.mean * 3.6 ORDER BY link_dir) AS tts,
            ARRAY_AGG(links.length ORDER BY link_dir) AS lengths
        FROM here.ta_path AS ta
        JOIN time_bins AS tb ON ta.tx >= tb.start_time AND ta.tx < tb.end_time
        JOIN segments AS links USING (link_dir)
        WHERE
            ta.dt >= %1$L::date
            AND ta.dt < %1$L::date + interval '1 day'
        GROUP BY
            links.segment_id,
            tb.time_grp,
            ta.tx,
            links.total_length
       WINDOW w AS (
            PARTITION BY links.segment_id, tb.time_grp
            ORDER BY ta.tx
       )
    ),
    
    dynamic_bin_options AS (
        --within each segment/hour, generate all possible forward looking bin combinations
        --don't generate options for bins with sufficient length
        --also don't generate options past the next bin with 80%% length
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
            s5b.total_length,
            dbo.tx AS dt_start,
            --exclusive end bin
            s5b_end.tx + interval '5 minutes' AS dt_end,
            unnested.link_dir,
            unnested.len,
            AVG(unnested.tt) AS tt, --avg TT for each link_dir
            SUM(s5b.num_obs) AS num_obs --sum of here.ta_path sample_size for each link_dir
        FROM dynamic_bin_options AS dbo
        LEFT JOIN segment_5min_bins AS s5b
            ON s5b.time_grp = dbo.time_grp
            AND s5b.segment_id = dbo.segment_id
            AND s5b.bin_rank >= dbo.start_bin
            AND s5b.bin_rank <= dbo.end_bin
        --this join is used to get the tx info about the last bin only
        LEFT JOIN segment_5min_bins AS s5b_end
            ON s5b_end.time_grp = dbo.time_grp
            AND s5b_end.segment_id = dbo.segment_id
            AND s5b_end.bin_rank = dbo.end_bin,
        --unnest all the observations from individual link_dirs to reaggregate them within new dynamic bin
        UNNEST(s5b.link_dirs, s5b.lengths, s5b.tts) AS unnested(link_dir, len, tt)
        --dynamic bins should not exceed one hour (dt_end <= dt_start + 1 hr)
        WHERE s5b_end.tx + interval '5 minutes' <= dbo.tx + interval '1 hour'
        GROUP BY
            dbo.time_grp,
            dbo.segment_id,
            s5b.total_length,
            dbo.tx, --stard_bin
            s5b_end.tx, --end_bin
            unnested.link_dir,
            unnested.len
    )
    
    --this query contains overlapping values which get eliminated
    --via on conflict with the exclusion constraint on congestion_raw_segments table.
    INSERT INTO gwolofs.congestion_raw_segments (
        dt, time_grp, segment_id, bin_range, tt, num_obs
    )
    --distinct on ensures only the shortest option gets proposed for insert
    SELECT DISTINCT ON (time_grp, segment_id, dt_start)
        dt_start::date AS dt,
        time_grp,
        segment_id,
        tsrange(dt_start, dt_end, '[)') AS bin_range,
        total_length / SUM(len) * SUM(tt) AS tt,
        SUM(num_obs) AS num_obs --sum of here.ta_path sample_size for each segment
    FROM unnested_db_options
    GROUP BY
        time_grp,
        segment_id,
        dt_start,
        dt_end,
        total_length
    HAVING SUM(len) >= 0.8 * total_length
    ORDER BY
        time_grp,
        segment_id,
        dt_start,
        dt_end --uses the option that ends first
    --exclusion constraint + ordered insert to prevent overlapping bins
    ON CONFLICT ON CONSTRAINT congestion_raw_segments_exclude
    DO NOTHING;
    $$, 
    start_date,
    congestion_network_table
    );

END;
$BODY$;

ALTER FUNCTION gwolofs.congestion_day_hr_segment_agg(date)
OWNER TO gwolofs;

COMMENT ON FUNCTION gwolofs.congestion_day_hr_segment_agg(date)
IS 'Dynamic bin aggregation of the congestion network by hourly periods.';
