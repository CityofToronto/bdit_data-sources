CREATE OR REPLACE FUNCTION here.create_weekly_partitions(
    yr int,
    schem text,
    tbl text
)
RETURNS void
LANGUAGE plpgsql
COST 100
VOLATILE PARALLEL UNSAFE
AS $$
DECLARE
    v_week_end date;
    v_partition_yr text := tbl || '_' || yr::text;
    v_partition_week text;
    v_week_num int;

    -- First Monday on or before Jan 1 of the given year
    v_week_start date := date_trunc('week', make_date(yr, 1, 1))::DATE;

BEGIN

    EXECUTE format(
        'CREATE TABLE %1$I.%2$I PARTITION OF %1$I.%3$I
            FOR VALUES FROM (%4$L) TO (%4$L::date + interval ''52 weeks'') PARTITION BY RANGE (tx)',
        schem,
        v_partition_yr,
        tbl,
        v_week_start
    );

    -- Iterate over every Monday whose week overlaps the target year
    LOOP
    
        -- Stop once the week starts on or after Jan 1 of the next year
        EXIT WHEN v_week_start + interval '7 days' >= make_date(yr + 1, 1, 1);

        -- Find week number for partition name
        v_week_num := EXTRACT(WEEK FROM v_week_start)::INT;

        -- Partition name: <table>_<year>_w<nn>  e.g. my_table_2024_w01
        v_partition_week := format('%s_w%s',
            v_partition_yr,
            LPAD(v_week_num::TEXT, 2, '0')
        );

        EXECUTE format(
            'CREATE TABLE %1$I.%2$I PARTITION OF %1$I.%3$I
                FOR VALUES FROM (%4$L) TO (%4$L::date + 7)',
            schem,
            v_partition_week,
            v_partition_yr,
            v_week_start
        );

        RAISE NOTICE 'Created partition "%" for % -> % (exclusive)',
            v_partition_week, v_week_start, v_week_start + interval '7 days';

        v_week_start := v_week_start + interval '7 days';
    END LOOP;

    RAISE NOTICE 'Done - weekly partitions created for % on table "%".',
        yr, tbl;
END;
$$;

ALTER FUNCTION here.create_weekly_partitions OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here.create_weekly_partitions TO here_bot;

-- Example usage
--SELECT here.create_weekly_partitions(2025, 'here', 'ta_path_hm');
