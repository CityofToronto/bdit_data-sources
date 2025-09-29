-- FUNCTION: gwolofs.congestion_return_dynamic_bins(text, date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean) --noqa: LT05

-- DROP FUNCTION IF EXISTS gwolofs.congestion_return_dynamic_bins(text, date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean); --noqa: LT05

CREATE OR REPLACE FUNCTION gwolofs.congestion_return_dynamic_bins(
    start_date date,
    end_date date,
    start_tod time without time zone,
    end_tod time without time zone,
    dow_list integer [],
    node_start bigint,
    node_end bigint,
    holidays boolean
)
RETURNS TABLE (
    dt date,
    time_grp timerange,
    corridor_id smallint,
    bin_range tsrange,
    tt real,
    num_obs integer,
    hr smallint
)
LANGUAGE plpgsql
COST 100
VOLATILE PARALLEL RESTRICTED
AS $BODY$

DECLARE
map_version text;

BEGIN

--using a temp table to aply the exclusion constraint should prevent the
--insert from getting bogged down by large constraint on main table over time
DROP TABLE IF EXISTS congestion_raw_corridors_temp;
CREATE TEMPORARY TABLE congestion_raw_corridors_temp (
    dt date GENERATED ALWAYS AS (lower(bin_range)) STORED,
    corridor_id smallint,
    time_grp timerange NOT NULL,
    bin_range tsrange NOT NULL,
    tt real,
    num_obs integer,
    hr smallint GENERATED ALWAYS AS (date_part('hour', lower(bin_range) + (upper(bin_range) - lower(bin_range))/2)) STORED,
    CONSTRAINT congestion_raw_corridors_exclude_temp EXCLUDE USING gist (
        bin_range WITH &&,
        corridor_id WITH =,
        time_grp WITH =
    )
);

SELECT gwolofs.congestion_select_map_version(
    congestion_return_dynamic_bins.start_date,
    congestion_return_dynamic_bins.end_date,
    'path'
) INTO map_version;

RETURN QUERY EXECUTE FORMAT(
    $$
    WITH corridor AS (
        SELECT
            ccc.corridor_id,
            unnested.link_dir,
            unnested.length,
            ccc.total_length
        FROM gwolofs.congestion_cache_corridor(%1$L, %2$L, %3$L) AS ccc,
        UNNEST(
            ccc.link_dirs,
            ccc.lengths
        ) AS unnested(link_dir, length)
    ),
    
    segment_5min_bins AS (
        SELECT
            seg.corridor_id,
            ta.tx,
            seg.total_length,
            tsrange(
                ta.dt + %4$L::time,
                ta.dt + %5$L::time, '[)') AS time_grp,
            RANK() OVER w AS bin_rank,
            SUM(seg.length) / seg.total_length AS sum_length,
            SUM(seg.length) AS length_w_data,
            SUM(seg.length / ta.mean * 3.6) AS unadjusted_tt,
            SUM(sample_size) AS num_obs,
            ARRAY_AGG(ta.link_dir ORDER BY ta.link_dir) AS link_dirs,
            ARRAY_AGG(seg.length / ta.mean * 3.6 ORDER BY ta.link_dir) AS tts,
            ARRAY_AGG(seg.length ORDER BY ta.link_dir) AS lengths
        FROM here.ta_path AS ta
        JOIN corridor AS seg USING (link_dir)
        LEFT JOIN ref.holiday USING (dt)
        WHERE
            (
                ta.tod >= %4$L
                AND --{ToD_and_or}
                ta.tod < %5$L
            )
            AND date_part('isodow', ta.dt) = ANY(%6$L::int[])
            AND ta.dt >= %7$L
            AND ta.dt < %8$L
            AND (%9$L OR holiday.dt IS NULL) --holiday clause
        GROUP BY
            seg.corridor_id,
            ta.tx,
            ta.dt,
            seg.total_length
       WINDOW w AS (
            PARTITION BY seg.corridor_id, ta.dt
            ORDER BY ta.tx
       )
    ),
    
    dynamic_bin_options AS (
        --within each corridor/hour, generate all possible forward looking bin combinations
        --don't generate options for bins with sufficient length
        --also don't generate options past the next bin with 80%% length
        SELECT
            tx,
            corridor_id,
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
            PARTITION BY corridor_id, time_grp
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
        --dynamic bins should not exceed one hour (dt_end <= dt_start + 1 hr)
        WHERE s5b_end.tx + interval '5 minutes' <= dbo.tx + interval '30 minutes'
        GROUP BY
            s5b.corridor_id,
            dbo.time_grp,
            s5b.total_length,
            dbo.tx, --stard_bin
            s5b_end.tx, --end_bin
            unnested.link_dir,
            unnested.len
    )
    
    --this query contains overlapping values which get eliminated
    --via on conflict with the exclusion constraint on congestion_raw_segments table.
    INSERT INTO congestion_raw_corridors_temp AS inserted (
        time_grp, corridor_id, bin_range, tt, num_obs
    )
    --distinct on ensures only the shortest option gets proposed for insert
    SELECT DISTINCT ON (dt_start)
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
    ON CONFLICT ON CONSTRAINT congestion_raw_corridors_exclude_temp
    DO NOTHING
    RETURNING
        inserted.dt, inserted.time_grp, inserted.corridor_id,
        inserted.bin_range, inserted.tt, inserted.num_obs, inserted.hr;
            
    $$,
    node_start, node_end, map_version, --segment CTE
    start_tod, end_tod, --segment_5min_bins CTE SELECT
    dow_list, start_date, end_date, holidays --segment_5min_bins CTE WHERE
);

END;
$BODY$;

ALTER FUNCTION gwolofs.congestion_return_dynamic_bins(
    date, date, time without time zone,
    time without time zone, integer [], bigint, bigint, boolean
)
OWNER TO gwolofs;

COMMENT ON FUNCTION gwolofs.congestion_return_dynamic_bins IS
'Returns the dynamic binning results for a request.';

-- overload the function for more straightforward situation of daily corridor agg
CREATE OR REPLACE FUNCTION gwolofs.congestion_return_dynamic_bins_daily(
    start_date date,
    node_start bigint,
    node_end bigint
)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE PARALLEL UNSAFE
AS
$BODY$
SELECT gwolofs.congestion_return_dynamic_bins(
    start_date := congestion_return_dynamic_bins_daily.start_date,
    end_date := congestion_return_dynamic_bins_daily.start_date + 1,
    start_tod := '00:00'::time without time zone,
    end_tod := '24:00'::time without time zone,
    dow_list := ARRAY[extract('isodow' from congestion_return_dynamic_bins_daily.start_date)]::int[],
    node_start := congestion_return_dynamic_bins_daily.node_start,
    node_end := congestion_return_dynamic_bins_daily.node_end,
    holidays := True)
$BODY$;

COMMENT ON FUNCTION gwolofs.congestion_return_dynamic_bins_daily
IS 'A simplified version of `congestion_return_dynamic_bins` for aggregating entire days of data.'
