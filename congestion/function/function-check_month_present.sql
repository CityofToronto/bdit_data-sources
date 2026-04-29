CREATE OR REPLACE FUNCTION here_agg.check_month_present(
    mnth date
)
RETURNS TABLE (
    _check boolean,
    _summary text
) AS $$

    WITH distinct_days AS (
        SELECT DISTINCT dt
        FROM here_agg.raw_segments
        WHERE
            dt >= check_month_present.mnth --noqa: TMP
            AND dt < check_month_present.mnth + interval '1 month' --noqa: TMP
    )
    
    SELECT
        COUNT(*) = 0 AS _check,
        'The following days are missing from `congestion_raw_segments`: '
        || string_agg(dates.dt::date::text, ', ') AS _summary
    FROM
        generate_series(
            check_month_present.mnth,
            --one day before start of next month
            (check_month_present.mnth + interval '1 month')::date - 1,
            '1 day'
        ) AS dates (dt)
    LEFT JOIN distinct_days USING (dt)
    WHERE distinct_days.dt IS NULL;
    
$$
LANGUAGE sql
SECURITY INVOKER;

ALTER FUNCTION here_agg.check_month_present OWNER TO here_admins;

--SELECT _check, _summary FROM here_agg.check_month_present('2026-04-01')