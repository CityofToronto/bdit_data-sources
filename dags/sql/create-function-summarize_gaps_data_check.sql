
CREATE OR REPLACE FUNCTION public.summarize_gaps_data_check(
    start_date date,
    end_date date,
    id_col text,
    dt_col text,
    sch_name text,
    tbl_name text,
    gap_threshold interval,
    default_bin interval
)
RETURNS TABLE (
    _check boolean, summ text, gaps text []
)
LANGUAGE plpgsql
COST 100
VOLATILE

AS $BODY$
BEGIN
    RETURN QUERY EXECUTE FORMAT($$
        WITH summarized_gaps AS (
            SELECT sensor_id_col, gap_start, gap_end
            FROM public.generic_find_gaps(
                %L::date,  --start_date
                %L::date, --end_date
                %L::text, --id_col
                %L::text, --dt_col
                %L::text, --sch_name
                %L::text, --tbl_name
                %L::interval, --gap_threshold
                %L::interval, --default_bin
                null::text --id_col_dtype
            )
        )

        SELECT
            NOT(COUNT(summarized_gaps.*) > 0) AS check,
            CASE WHEN COUNT(summarized_gaps.*) = 1 THEN 'There was ' ELSE 'There were ' END ||
                COALESCE(COUNT(summarized_gaps.*), 0) ||
                CASE WHEN COUNT(summarized_gaps.*) = 1 THEN ' gap' ELSE ' gaps' END
                || ' larger than ' || %L || '.' AS summ, --gap_threshold
            array_agg(summarized_gaps || chr(10)) AS gaps
        FROM summarized_gaps
    $$,
    start_date, end_date, id_col, dt_col, sch_name, tbl_name, gap_threshold, default_bin, gap_threshold
    );
END;
$BODY$;

ALTER FUNCTION public.summarize_gaps_data_check
OWNER TO gwolofs;

GRANT EXECUTE ON FUNCTION public.summarize_gaps_data_check TO bdit_humans;
GRANT EXECUTE ON FUNCTION public.summarize_gaps_data_check TO bdit_bots;

COMMENT ON FUNCTION public.summarize_gaps_data_check
IS 'Summarize results of FUNCTION public.generic_find_gaps for an airflow data check.';
