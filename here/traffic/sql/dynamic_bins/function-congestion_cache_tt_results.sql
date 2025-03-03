-- FUNCTION: gwolofs.congestion_cache_tt_results(text, date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean) --noqa: LT05

-- DROP FUNCTION IF EXISTS gwolofs.congestion_cache_tt_results(text, date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean); --noqa: LT05

CREATE OR REPLACE FUNCTION gwolofs.congestion_cache_tt_results(
    uri_string text,
    start_date date,
    end_date date,
    start_tod time without time zone,
    end_tod time without time zone,
    dow_list integer [],
    node_start bigint,
    node_end bigint,
    holidays boolean
)
RETURNS void
LANGUAGE plpgsql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

DECLARE map_version text;

BEGIN

SELECT gwolofs.congestion_select_map_version(
    congestion_cache_tt_results.start_date,
    congestion_cache_tt_results.end_date
) INTO map_version;

EXECUTE format(
    $$
    WITH segment AS (
        SELECT
            corridor_id,
            unnested.link_dir,
            unnested.length,
            total_length
        FROM gwolofs.congestion_cache_corridor(%L, %L, %L),
        UNNEST(
            congestion_cache_corridor.link_dirs,
            congestion_cache_corridor.lengths
        ) AS unnested(link_dir, length)
    ),
    
    segment_5min_bins AS (
        SELECT
            seg.corridor_id,
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
            ARRAY_AGG(ta.link_dir ORDER BY ta.link_dir) AS link_dirs,
            ARRAY_AGG(seg.length / ta.mean * 3.6 ORDER BY ta.link_dir) AS tts,
            ARRAY_AGG(seg.length ORDER BY ta.link_dir) AS lengths
        FROM here.ta_path AS ta
        JOIN segment AS seg USING (link_dir)
        WHERE
            (
                ta.tod >= %L
                AND --{ToD_and_or}
                ta.tod < %L
            )
            AND date_part('isodow', ta.dt) = ANY(%L::int[])
            AND ta.dt >= %L
            AND ta.dt < %L
            /*--{holiday_clause}
            AND NOT EXISTS (
                SELECT 1 FROM ref.holiday WHERE ta.dt = holiday.dt
            )*/
        GROUP BY
            ta.tx,
            ta.dt,
            seg.total_length,
            seg.corridor_id
       WINDOW w AS (
            PARTITION BY seg.corridor_id, ta.dt
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
                    ELSE COALESCE(
                        MIN(bin_rank) FILTER (WHERE sum_length >= 0.8) OVER w,
                        MAX(bin_rank) OVER w
                    )
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
            s5b.corridor_id,
            dbo.time_grp,
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
            AND s5b.bin_rank >= dbo.start_bin
            AND s5b.bin_rank <= dbo.end_bin
        --this join is used to get the tx info about the last bin only
        LEFT JOIN segment_5min_bins AS s5b_end
            ON s5b_end.time_grp = dbo.time_grp
            AND s5b_end.bin_rank = dbo.end_bin,
        --unnest all the observations from individual link_dirs to reaggregate them within new dynamic bin
        UNNEST(s5b.link_dirs, s5b.lengths, s5b.tts) AS unnested(link_dir, len, tt)
        GROUP BY
            s5b.corridor_id,
            dbo.time_grp,
            s5b.total_length,
            dbo.tx, --stard_bin
            s5b_end.tx, --end_bin
            unnested.link_dir,
            unnested.len
        --dynamic bins should not exceed one hour (dt_end <= dt_start + 1 hr)
        --HAVING MAX(s5b.tx) + interval '5 minutes' <= dbo.tx + interval '1 hour'
    )
    
    INSERT INTO gwolofs.congestion_raw_corridors (
        uri_string, dt, time_grp, corridor_id,  bin_range, tt, num_obs
    )
    --this query contains overlapping values which get eliminated
    --via on conflict with the exclusion constraint on congestion_raw_segments table.
    SELECT DISTINCT ON (dt_start) --distinct on ensures only the shortest option gets proposed for insert
        %L,
        dt_start::date AS dt,
        timerange(lower(time_grp)::time, upper(time_grp)::time, '[)') AS time_grp,
        corridor_id,
        tsrange(dt_start, dt_end, '[)') AS bin_range,
        total_length / SUM(len) * SUM(tt) AS tt,
        SUM(num_obs) AS num_obs --sum of here.ta_path sample_size for each segment
    FROM unnested_db_options
    GROUP BY
        time_grp,
        corridor_id,
        dt_start,
        dt_end,
        total_length
    HAVING SUM(len) >= 0.8 * total_length
    ORDER BY
        dt_start,
        dt_end
    --exclusion constraint + ordered insert to prevent overlapping bins
    ON CONFLICT ON CONSTRAINT congestion_raw_corridors_exclude
    DO NOTHING;
    $$,
    node_start, node_end, map_version, --segment CTE
    start_tod, end_tod, --segment_5min_bins CTE SELECT
    start_tod, end_tod, dow_list, start_date, end_date, --segment_5min_bins CTE WHERE
    congestion_cache_tt_results.uri_string, congestion_cache_tt_results.uri_string --INSERT
);

END;
$BODY$;

ALTER FUNCTION gwolofs.congestion_cache_tt_results(
    text, date, date, time without time zone,
    time without time zone, integer [], bigint, bigint, boolean
)
OWNER TO gwolofs;

COMMENT ON FUNCTION gwolofs.congestion_cache_tt_results IS
'Caches the dynamic binning results for a request.';
