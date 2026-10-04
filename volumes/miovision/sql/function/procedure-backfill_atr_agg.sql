--DROP PROCEDURE IF EXISTS miovision_api.backfill_atr_agg;
CREATE OR REPLACE PROCEDURE miovision_api.backfill_atr_agg(
    p_start_date date,
    p_end_date date
)
LANGUAGE plpgsql
AS $$
DECLARE
    date_record DATE;
    all_checks_passed BOOLEAN;
BEGIN

    FOR date_record IN
        SELECT generate_series(
            p_start_date,
            p_end_date,
            '1 day'::interval
        )::date
    LOOP
        RAISE NOTICE 'Working on date: %', date_record;

        EXECUTE 'SELECT miovision_api.aggregate_15_min_atr($1, $1 + 1);' USING date_record;

        COMMIT;

    END LOOP;
END $$;

COMMENT ON PROCEDURE miovision_api.backfill_atr_agg
IS 'A procedure to backfill Miovision ATR aggregation for a range of dates,
committing one day at a time.';

--example
--CALL miovision_api.backfill_atr_agg('2019-01-27'::date, '2020-01-01'::date)
