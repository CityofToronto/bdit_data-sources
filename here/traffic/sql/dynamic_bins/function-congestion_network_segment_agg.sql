-- FUNCTION: gwolofs.congestion_network_segment_agg(date)

-- DROP FUNCTION IF EXISTS gwolofs.congestion_network_segment_agg(date);

CREATE OR REPLACE FUNCTION gwolofs.congestion_network_segment_agg(
    start_date date
)
RETURNS void
LANGUAGE plpgsql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

DECLARE
    map_version text := gwolofs.congestion_select_map_version(start_date, start_date + 1, 'path');
    congestion_network_table text := 'network_links_' || map_version
    || CASE map_version WHEN '23_4' THEN '_geom' ELSE '' END; --temp fix version

BEGIN

--using a temp table to aply the exclusion constraint should prevent the
--insert from getting bogged down by large constraint on main table over time
DROP TABLE IF EXISTS congestion_raw_segments_temp;
CREATE TEMPORARY TABLE congestion_raw_segments_temp (
    segment_id integer NOT NULL,
    bin_start timestamp without time zone NOT NULL,
    bin_range tsrange NOT NULL,
    tt numeric,
    num_obs integer,
    CONSTRAINT congestion_raw_segments_exclude_temp EXCLUDE USING gist (
        bin_range WITH &&,
        segment_id WITH =
    )
) ON COMMIT DROP;

EXECUTE FORMAT(
    $$
        DROP TABLE IF EXISTS segment_5min_bins;
        CREATE TEMP TABLE segment_5min_bins ON COMMIT DROP AS
        SELECT
            seg.segment_id,
            ta.tx,
            seg.segment_length AS total_length,
            ROW_NUMBER() OVER w AS bin_rank,
            SUM(seg.length) / seg.segment_length AS sum_length,
            SUM(seg.length) AS length_w_data,
            SUM(seg.length / ta.mean * 3.6) AS unadjusted_tt,
            SUM(sample_size) AS num_obs,
            ARRAY_AGG(ta.link_dir ORDER BY ta.link_dir) AS link_dirs,
            ARRAY_AGG(lat.tt ORDER BY ta.link_dir) AS tts,
            ARRAY_AGG(seg.length ORDER BY ta.link_dir) AS lengths
        FROM here.ta_path AS ta
        JOIN congestion.%1$I AS seg USING (link_dir),
        LATERAL (
            SELECT seg.length / ta.mean * 3.6 AS tt
        ) AS lat
        WHERE
            ta.dt >= %2$L::date
            AND ta.dt < %2$L::date + interval '1 day'
        GROUP BY
            seg.segment_id,
            ta.tx,
            seg.segment_length
        WINDOW w AS (
            PARTITION BY seg.segment_id
            ORDER BY ta.tx
        );
    $$, congestion_network_table, start_date);

    CREATE INDEX idx_s5b_segment_rank ON segment_5min_bins(segment_id, bin_rank);
    CREATE INDEX idx_s5b_segment_tx ON segment_5min_bins(segment_id, tx);
    ANALYZE segment_5min_bins;
    
    --within each segment/hour, generate all possible forward looking bin combinations
    --don't generate options for bins with sufficient length
    --also don't generate options past the next bin with 80% length
    DROP TABLE IF EXISTS dynamic_bin_options;
    CREATE TEMP TABLE dynamic_bin_options ON COMMIT DROP AS
    SELECT
            tx,
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
                    ELSE COALESCE(
                        MIN(bin_rank) FILTER (WHERE sum_length >= 0.8) OVER w,
                        MAX(bin_rank) OVER w
                    )
                END,
                1
            ) AS end_bin
        FROM segment_5min_bins
        WINDOW w AS (
            PARTITION BY segment_id
            ORDER BY tx
            --look only forward for end_bin options
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        );
    
    CREATE INDEX idx_dbo_composite ON dynamic_bin_options(segment_id, start_bin, end_bin);
    CREATE INDEX idx_dbo_segment_tx ON dynamic_bin_options(segment_id, tx);
    ANALYZE dynamic_bin_options;
    
    WITH unnested_db_options AS (
        SELECT
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
            ON s5b.segment_id = dbo.segment_id
            AND s5b.bin_rank >= dbo.start_bin
            AND s5b.bin_rank <= dbo.end_bin
        --this join is used to get the tx info about the last bin only
        LEFT JOIN segment_5min_bins AS s5b_end
            ON s5b_end.segment_id = dbo.segment_id
            AND s5b_end.bin_rank = dbo.end_bin,
        --unnest all the observations from individual link_dirs to reaggregate them within new dynamic bin
        UNNEST(s5b.link_dirs, s5b.lengths, s5b.tts) AS unnested(link_dir, len, tt)
        --dynamic bins should not exceed 15 minutes (dt_end <= dt_start + 15 min)
        WHERE s5b_end.tx + interval '5 minutes' <= dbo.tx + interval '15 minutes'
        GROUP BY
            dbo.segment_id,
            s5b.total_length,
            dbo.tx, --stard_bin
            s5b_end.tx, --end_bin
            unnested.link_dir,
            unnested.len
    )

    --this query contains overlapping values which get eliminated
    --via on conflict with the exclusion constraint on congestion_raw_segments table.
    INSERT INTO congestion_raw_segments_temp AS inserted (
        bin_start, segment_id, bin_range, tt, num_obs
    )
    --distinct on ensures only the shortest option gets proposed for insert
    SELECT DISTINCT ON (segment_id, dt_start)
        dt_start AS bin_start,
        segment_id,
        tsrange(dt_start, dt_end, '[)') AS bin_range,
        total_length / SUM(len) * SUM(tt) AS tt,
        SUM(num_obs) AS num_obs --sum of here.ta_path sample_size for each segment
    FROM unnested_db_options
    GROUP BY
        segment_id,
        dt_start,
        dt_end,
        total_length
    HAVING SUM(len) >= 0.8 * total_length
    ORDER BY
        segment_id,
        dt_start,
        dt_end --uses the option that ends first
    --exclusion constraint + ordered insert to prevent overlapping bins
    ON CONFLICT ON CONSTRAINT congestion_raw_segments_exclude_temp
    DO NOTHING;
    
    ANALYZE congestion_raw_segments_temp;
    
    INSERT INTO gwolofs.congestion_raw_segments (
        dt, bin_start, segment_id, bin_range, tt, num_obs, hr
    )
    SELECT
        bin_start::date AS dt,
        bin_start,
        segment_id,
        bin_range,
        tt::real,
        num_obs,
        date_part('hour', lower(bin_range) + (upper(bin_range) - lower(bin_range))/2) AS hr
    FROM congestion_raw_segments_temp
    ON CONFLICT DO NOTHING;

    DROP TABLE congestion_raw_segments_temp;

END;
$BODY$;

ALTER FUNCTION gwolofs.congestion_network_segment_agg(date)
OWNER TO gwolofs;

GRANT EXECUTE ON FUNCTION gwolofs.congestion_network_segment_agg(date) TO congestion_bot;

COMMENT ON FUNCTION gwolofs.congestion_network_segment_agg(date)
IS 'Dynamic bin aggregation of the congestion network by hour and time periods. 
Takes around 10 minutes to run for one day (hourly and period based aggregation)';
