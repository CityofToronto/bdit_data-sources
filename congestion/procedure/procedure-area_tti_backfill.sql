CREATE OR REPLACE PROCEDURE here_agg.area_tti_backfill(
    p_start_date date,
    p_end_date date
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_date date := p_start_date;
BEGIN
    WHILE v_current_date < p_end_date LOOP
       
        RAISE NOTICE '[%] [%] Running area_tti_agg', clock_timestamp(), v_current_date;

        --refresh tt for the entire day
        PERFORM here_agg.area_tti_agg(
            v_current_date
        );

        -- Commit after each day's work
        COMMIT;

        v_current_date := v_current_date + interval '1 day';

    END LOOP;

    RAISE NOTICE '[%] Completed aggregations from % to %', clock_timestamp(), p_start_date, p_end_date;
END;
$$;

ALTER PROCEDURE here_agg.area_tti_backfill OWNER TO here_admins;

--CALL here_agg.area_tti_backfill('2026-01-01', '2026-06-23');
