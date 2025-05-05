CREATE OR REPLACE FUNCTION here.ta_path_daily_summary_insert(dt date)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

    INSERT INTO here.ta_path_daily_summary (
        dt, row_count, avg_speed, sum_sample_size, num_link_dirs
    )
    SELECT
        dt,
        COUNT(*) AS row_count,
        AVG(mean) AS avg_speed,
        SUM(sample_size) AS sum_sample_size,
        COUNT(DISTINCT link_dir) AS num_link_dirs
    FROM here.ta_path
    WHERE dt = ta_path_daily_summary_insert.dt
    GROUP BY dt
    ON CONFLICT ON CONSTRAINT ta_path_daily_summary_pkey
    DO UPDATE
    SET (row_count, avg_speed, sum_sample_size, num_link_dirs) = (
        excluded.row_count, excluded.avg_speed, excluded.sum_sample_size, excluded.num_link_dirs
    );
    
    INSERT INTO here.ta_path_daily_summary_link_dir (
        link_dir, dts, row_counts, sum_sample_size
    )
    SELECT
        link_dir,
        ARRAY[dt]::date[] AS dts,
        ARRAY[COUNT(*)]::int[] AS row_counts,
        ARRAY[SUM(sample_size)]::int[] AS sum_sample_size
    FROM here.ta_path
    WHERE dt = ta_path_daily_summary_insert.dt
    GROUP BY link_dir, dt

    ON CONFLICT ON CONSTRAINT ta_path_daily_summary_link_dir_pkey
    --append to existing arrays
    DO UPDATE SET
        dts = EXCLUDED.dts || ta_path_daily_summary_link_dir.dts,
        row_counts = EXCLUDED.row_counts || ta_path_daily_summary_link_dir.row_counts,
        sum_sample_size = EXCLUDED.sum_sample_size || ta_path_daily_summary_link_dir.sum_sample_size;
    
    --trim ARRAYS
    UPDATE here.ta_path_daily_summary_link_dir
    SET dts = trim_array(dts, array_length(dts, 1)-60)
    WHERE array_length(dts, 1) > 60;
    UPDATE here.ta_path_daily_summary_link_dir
    SET row_counts = trim_array(row_counts, array_length(row_counts, 1)-60)
    WHERE array_length(row_counts, 1) > 60;
    UPDATE here.ta_path_daily_summary_link_dir
    SET sum_sample_size = trim_array(sum_sample_size, array_length(sum_sample_size, 1)-60)
    WHERE array_length(sum_sample_size, 1) > 60;
    
$BODY$;

COMMENT ON FUNCTION here.ta_path_daily_summary_insert IS
'This function is called by an Airflow task after insert into 
here.ta_path. It uses newly inserted rows to update the summary table for 
use in data checks.';

GRANT EXECUTE ON FUNCTION here.ta_path_daily_summary_insert TO here_bot;

ALTER FUNCTION here.ta_path_daily_summary_insert OWNER TO here_admins;