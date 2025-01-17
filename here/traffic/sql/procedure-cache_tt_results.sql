-- PROCEDURE: gwolofs.cache_tt_results(date, date, time without time zone, time without time zone, integer[], text[], boolean)

-- DROP PROCEDURE IF EXISTS gwolofs.cache_tt_results(date, date, time without time zone, time without time zone, integer[], text[], boolean);

CREATE OR REPLACE PROCEDURE gwolofs.cache_tt_results(
    IN start_date date,
    IN end_date date,
    IN start_tod time without time zone,
    IN end_tod time without time zone,
    IN dow_list integer[],
    IN link_dirs text[],
    IN holidays boolean
)
LANGUAGE 'plpgsql'
AS $BODY$

BEGIN
EXECUTE format(
    $$
    WITH segment AS (
        SELECT
            uid AS segment_uid,
            unnested.link_dir,
            unnested.length,
            tt_segments.total_length
        FROM gwolofs.tt_segments,
        UNNEST(tt_segments.link_dirs, tt_segments.lengths) AS unnested(link_dir, length)
        WHERE link_dirs = %L
    ),
    
    segment_5min_bins AS (
        SELECT
            seg.segment_uid,
            ta.tx,
            seg.total_length,
            tsrange(
                ta.dt + %L::time,
                ta.dt + %L::time, '[)') AS time_grp,
            RANK() OVER w AS bin_rank,
            SUM(seg.length) / seg.total_length AS sum_length,
            SUM(seg.length) AS length_w_data,
            SUM(seg.length / ta.mean * 3.6) AS unadjusted_tt,
            SUM(sample_size) AS num_obs,
            ARRAY_AGG(ta.link_dir ORDER BY link_dir) AS link_dirs,
            ARRAY_AGG(seg.length / ta.mean * 3.6 ORDER BY link_dir) AS tts,
            ARRAY_AGG(seg.length ORDER BY link_dir) AS lengths
        FROM here.ta_path AS ta
        JOIN segment AS seg USING (link_dir)
        WHERE
            (
                tod >= %L
                AND --{ToD_and_or}
                tod < %L
            )
            AND date_part('isodow', dt) = ANY(%L)
            AND dt >= %L
            AND dt < %L
            /*--{holiday_clause}
            AND NOT EXISTS (
                SELECT 1 FROM ref.holiday WHERE ta.dt = holiday.dt
            )*/
        GROUP BY
            ta.tx,
            ta.dt,
            seg.total_length,
            segment_uid
       WINDOW w AS (
            PARTITION BY seg.segment_uid, ta.dt
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
            PARTITION BY time_grp
            ORDER BY tx
            --look only forward for end_bin options
            RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )
    ),
    
    unnested_db_options AS (
        SELECT
            s5b.segment_uid,
            dbo.time_grp,
            s5b.total_length,
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
            AND s5b.bin_rank >= dbo.start_bin
            AND s5b.bin_rank <= dbo.end_bin,
        --unnest all the observations from individual link_dirs to reaggregate them within new dynamic bin
        UNNEST(s5b.link_dirs, s5b.lengths, s5b.tts) AS unnested(link_dir, len, tt)
        --we need to use nested data to determine length for these multi-period bins
        WHERE dbo.start_bin != dbo.end_bin
        GROUP BY
            s5b.segment_uid,
            dbo.time_grp,
            s5b.total_length,
            dbo.tx,
            dbo.end_bin,
            unnested.link_dir,
            unnested.len
        --dynamic bins should not exceed one hour (dt_end <= dt_start + 1 hr)
        HAVING MAX(s5b.tx) + interval '5 minutes' <= dbo.tx + interval '1 hour'
    )
    
    INSERT INTO gwolofs.dynamic_binning_results (
        time_grp, segment_uid, dt_start, dt_end, bin_range, tt,
        unadjusted_tt, total_length, length_w_data, num_obs
    )
    --this query contains overlapping values which get eliminated
    --via on conflict with the exclusion constraint on congestion_raw_segments table.
    SELECT DISTINCT ON (dt_start)
        time_grp,
        segment_uid,
        dt_start,
        dt_end,
        tsrange(dt_start, dt_end, '[)') AS bin_range,
        total_length / SUM(len) * SUM(tt) AS tt,
        SUM(tt) AS unadjusted_tt,
        total_length,
        SUM(len) AS length_w_data,
        SUM(num_obs) AS num_obs --sum of here.ta_path sample_size for each segment
    FROM unnested_db_options
    GROUP BY
        time_grp,
        segment_uid,
        dt_start,
        dt_end,
        total_length
    HAVING SUM(len) >= 0.8 * total_length
    UNION
    --these 5 minute bins already have sufficient length
    --don't need to use nested data to validate.
    SELECT
        time_grp,
        segment_uid,
        tx AS dt_start,
        tx + interval '5 minutes' AS dt_end,
        tsrange(tx, tx + interval '5 minutes', '[)') AS bin_range,
        total_length / length_w_data * unadjusted_tt AS tt,
        unadjusted_tt,
        total_length,
        length_w_data,
        num_obs --sum of here.ta_path sample_size for each segment
    FROM segment_5min_bins
    --we do not need to use nested data to determine length here.
    WHERE sum_length >= 0.8
    ORDER BY
        dt_start,
        dt_end
    --exclusion constraint + ordered insert to prevent overlapping bins
    ON CONFLICT ON CONSTRAINT dynamic_bins_unique_temp
    DO NOTHING;
    $$,
    link_dirs, start_tod, end_tod, start_tod, end_tod, dow_list, start_date, end_date
);

END;
$BODY$;
ALTER PROCEDURE gwolofs.cache_tt_results(
    date, date, time without time zone, time without time zone, integer[], text[], boolean
)
OWNER TO gwolofs;
