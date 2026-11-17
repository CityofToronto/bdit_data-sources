--DROP FUNCTION IF EXISTS here.check_ta_present;
CREATE OR REPLACE FUNCTION here.check_ta_present(
    date_start date,
    date_end date,
    agg_type text DEFAULT ''
)
RETURNS TABLE (
    _check boolean,
    _summary text
) AS $BODY$

DECLARE
    ta_table text := 'ta' || CASE agg_type
        WHEN 'path' THEN '_path'
        WHEN 'path_hm' THEN '_path_hm'
        ELSE '' END;
    dt_col text := CASE agg_type
        WHEN 'path_hm' THEN 'tx'
        ELSE 'dt' END;

BEGIN

    RETURN QUERY EXECUTE FORMAT(
    $$

    WITH distinct_days AS (
        SELECT DISTINCT %4$I::date AS dt
        FROM here.%1$I
        WHERE
            %4$I >= %2$L
            AND %4$I < %3$L
    )
    
    SELECT
        COUNT(*) = 0 AS _check,
        'The following days are missing from `here.' || %1$L || '`: '
        || string_agg(dates.dt::date::text, ', ') AS _summary
    FROM
        generate_series(
            %2$L,
            %3$L::date - 1,
            '1 day'
        ) AS dates (dt)
    LEFT JOIN distinct_days USING (dt)
    WHERE distinct_days.dt IS NULL;
    $$, ta_table, check_ta_present.date_start, check_ta_present.date_end, dt_col);

END;
$BODY$
LANGUAGE plpgsql
SECURITY INVOKER;

ALTER FUNCTION here.check_ta_present OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here.check_ta_present TO public;

COMMENT ON FUNCTION here.check_ta_present IS
'Used to check if all days in a month of here.ta data. Can also work for ta_path and ta_path_hm with optional agg_type param (''path'' or ''path_hm'').';

--example usage:
--SELECT _check, _summary FROM here.check_ta_present('2026-02-01'::date, '2026-04-01'::date, 'path')
