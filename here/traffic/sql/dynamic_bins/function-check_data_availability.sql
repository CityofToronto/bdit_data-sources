CREATE OR REPLACE FUNCTION here_agg.check_data_availability(
    date_start date,
    date_end date
)
RETURNS TABLE (
    _check boolean,
    _summary text
) AS $$

WITH distinct_days AS (
    SELECT DISTINCT dt
    FROM here_agg.raw_segments
    WHERE
        dt >= date_start
        AND dt < date_end
)

SELECT
    COUNT(*) = 0 AS _check,
    'The following days are missing from `congestion_raw_segments`: '
    || string_agg(dates.dt::date::text, ', ') AS _summary
FROM generate_series(date_start, date_end - 1, '1 day') AS dates (dt)
LEFT JOIN distinct_days USING (dt)
WHERE distinct_days.dt IS NULL;

$$
LANGUAGE sql
SECURITY DEFINER;

ALTER FUNCTION here_agg.check_data_availability OWNER TO here_admins;
GRANT EXECUTE ON FUNCTION here_agg.check_data_availability TO here_bot;

--SELECT _check, _summary FROM here_agg.check_data_availability('2025-01-01', '2026-01-01')