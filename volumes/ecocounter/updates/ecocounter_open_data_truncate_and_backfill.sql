--- Backfill was performed on 11-19-2026 for issue #1491

--- TRUNCATE the tables
TRUNCATE TABLE ecocounter.open_data_daily_counts, ecocounter.open_data_15min_counts;

---run open_data functions on all years
DO $$
DECLARE
    year_ integer;
BEGIN
    FOR year_ IN
        SELECT DISTINCT EXTRACT(YEAR FROM datetime_bin)::int
        FROM ecocounter.counts_unfiltered
		ORDER BY 1
    LOOP
        RAISE NOTICE 'Backfilling: %', year_;
		PERFORM ecocounter.open_data_daily_counts_insert(year_);
		PERFORM ecocounter.open_data_15min_counts_insert(year_);
    END LOOP;
END $$;