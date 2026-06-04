CREATE OR REPLACE PROCEDURE here_agg.surgical_segment_backfill(
    p_start_date date,
    p_end_date date,
    p_segments bigint []
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_date date := p_start_date;
BEGIN
    WHILE v_current_date < p_end_date LOOP

        RAISE NOTICE 'Processing date: %', v_current_date;
        RAISE NOTICE '[%] Running dynamic binning', v_current_date;

        DELETE FROM here_agg.raw_segments WHERE segment_id = ANY(p_segments) AND dt = v_current_date::date;
        
        --segment dynamic binning
        PERFORM here_agg.network_segment_agg(
            v_current_date,
            p_segments
        );

        -- On the first day of the month, aggregate the previous months lookback stats (overnight, pkt)
        IF EXTRACT(DAY FROM v_current_date) = 1 THEN
            
            --this would have to be for the previous month, and have a delete statement preceding.
            --PERFORM here_agg.monthly_bootstrap(date_trunc('month', v_current_date::date)::date, p_segments);
            
            RAISE NOTICE '[%] Aggregating the previous months lookback stats (overnight, pkt)', v_current_date;

            --add pkt/overnight speeds for the segments
            PERFORM here_agg.agg_segment_6month_lookback(
                v_current_date,
                p_segments
            );

        END IF;
        
        --this would have to be for the previous week, and have a delete statement preceding.
        -- Weekly aggregation run every Saturdays
        /*IF EXTRACT(ISODOW FROM v_current_date) = 6 THEN
            PERFORM here_agg.weekly_bootstrap(date_trunc('month', v_current_date::date)::date, p_segments);
        END IF;*/

        -- Every day
        RAISE NOTICE '[%] Running daily aggregations', v_current_date;

        --store hourly avg speeds
        PERFORM here_agg.segment_travel_times_hrly_agg(
            v_current_date,
            p_segments
        );

        --refresh tt for the entire day
        PERFORM here_agg.area_tti_agg(
            v_current_date
        );

        -- Commit after each day's work
        COMMIT;

        v_current_date := v_current_date + interval '1 day';

    END LOOP;

    RAISE NOTICE 'Completed aggregations from % to %', p_start_date, p_end_date;
END;
$$;

ALTER PROCEDURE here_agg.surgical_segment_backfill OWNER TO here_admins;

--CALL here_agg.surgical_segment_backfill('2026-01-01', '2026-02-01', ARRAY[6805]::bigint [])
