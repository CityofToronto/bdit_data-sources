--noqa: disable=TMP, PRS
CREATE OR REPLACE FUNCTION public.generic_find_gaps(
    start_date date,
    end_date date,
    id_col text,
    dt_col text,
    sch_name text,
    tbl_name text,
    gap_threshold interval,
    default_bin interval,
    id_col_dtype anyelement DEFAULT NULL::int
)
RETURNS TABLE (
    sensor_id_col anyelement, gap_start timestamp, gap_end timestamp
)
LANGUAGE plpgsql
COST 100
VOLATILE

AS $BODY$
DECLARE
   resulttype regtype := pg_typeof(id_col_dtype);
BEGIN
    RETURN QUERY EXECUTE FORMAT($$
        WITH raw AS (
            SELECT DISTINCT
                CAST(%I AS %s) AS sensor_id_col, --id_col, resulttype
                %I AS dt_col --dt_col
            FROM %I.%I --sch_name, tbl_name
            WHERE
                %I >= %L::date - %L::interval --dt_col, start_date, gap_threshold
                AND %I < %L::date + '1 day'::interval --dt_col, end_date
        ),

        --add a start and end dt for each ID which appears in raw data
        fluffed_data AS (
            --synthetic data
            SELECT DISTINCT
                raw.sensor_id_col,
                bins.dt_col,
                --don't need to reduce gap width for synthetic data since that minute does not contain data
                '0 minutes'::interval AS gap_adjustment
            FROM raw, 
                (VALUES 
                    (%L::date - %L::interval), --start_date, gap_threshold
                    (%L::date + '1 day'::interval) --end_date
                ) AS bins(dt_col)

            UNION ALL

            SELECT
                sensor_id_col,
                dt_col,
                --need to reduce gap length by 1 minute for real data since that minute contains data
                %L::interval AS gap_adjustment --default_bin
            FROM raw
        ),

        --looks at sequential bins to identify gaps
        bin_times AS (
            SELECT 
                sensor_id_col,
                dt_col + sum(gap_adjustment) AS gap_start,
                LEAD(dt_col, 1) OVER (PARTITION BY sensor_id_col ORDER BY dt_col) AS gap_end,
                LEAD(dt_col, 1) OVER (PARTITION BY sensor_id_col ORDER BY dt_col)
                    - dt_col
                    --sum works because between 0 (synthetic) and 1 (real), we want 1 (implies real data)
                    - SUM(gap_adjustment)
                AS bin_gap
            FROM fluffed_data
            GROUP BY
                sensor_id_col, 
                dt_col
        )

        --summarize gaps by sensor.
        SELECT
            sensor_id_col,
            gap_start,
            gap_end
        FROM bin_times
        WHERE
            gap_end IS NOT NULL --NULL gap_end occurs at the end of the search period
            AND bin_gap >= %L::interval --gap_threshold
    $$,
    id_col, resulttype,
    dt_col,
    sch_name, tbl_name, 
    dt_col, start_date, gap_threshold,
    dt_col, end_date,
    start_date, gap_threshold,
    end_date,
    default_bin,
    gap_threshold);
    
END;
$BODY$;

ALTER FUNCTION public.generic_find_gaps
OWNER TO gwolofs;

GRANT EXECUTE ON FUNCTION public.generic_find_gaps TO bdit_humans;
GRANT EXECUTE ON FUNCTION public.generic_find_gaps TO bdit_bots;

COMMENT ON FUNCTION public.generic_find_gaps
IS '''A generic find_gaps function to find gaps in sensor data.
Includes a lookback of `gap_threshold` so it catches gaps overlapping the start of the interval.
Includes synthetic start/end time points for all sensors so we catch gaps that don''t start or
don''t end in the interval. Doesn''t catch sensors with no data in the interval.''';