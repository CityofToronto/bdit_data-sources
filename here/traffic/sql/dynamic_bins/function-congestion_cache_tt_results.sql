-- FUNCTION: here_agg.cache_tt_results(text, date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean) --noqa: LT05

-- DROP FUNCTION IF EXISTS here_agg.cache_tt_results(text, date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean); --noqa: LT05

CREATE OR REPLACE FUNCTION here_agg.cache_tt_results(
    uri_string text,
    start_date date,
    end_date date,
    start_tod time without time zone,
    end_tod time without time zone,
    dow_list integer [],
    node_start bigint,
    node_end bigint,
    holidays boolean
)
RETURNS void
LANGUAGE SQL
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

    --insert into the final table
    INSERT INTO here_agg.raw_corridors (
        uri_string, dt, time_grp, corridor_id,  bin_range, tt, num_obs, hr
    )
    SELECT
        cache_tt_results.uri_string,
        dt, time_grp, corridor_id, bin_range, tt, num_obs, hr
    FROM here_agg.return_dynamic_bins(
        cache_tt_results.start_date,
        cache_tt_results.end_date,
        cache_tt_results.start_tod,
        cache_tt_results.end_tod,
        cache_tt_results.dow_list,
        cache_tt_results.node_start,
        cache_tt_results.node_end,
        cache_tt_results.holidays
    )
    ON CONFLICT DO NOTHING;
    
$BODY$;

ALTER FUNCTION here_agg.cache_tt_results(
    text, date, date, time without time zone,
    time without time zone, integer [], bigint, bigint, boolean
)
OWNER TO here_admins;

COMMENT ON FUNCTION here_agg.cache_tt_results IS
'Caches the dynamic binning results for a request.';

-- overload the function for more straightforward situation of daily corridor agg
CREATE OR REPLACE FUNCTION here_agg.cache_tt_results_daily(
    start_date date,
    node_start bigint,
    node_end bigint
)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE PARALLEL UNSAFE
AS
$BODY$
SELECT here_agg.cache_tt_results(
    uri_string := NULL::text,
    start_date := cache_tt_results_daily.start_date,
    end_date := cache_tt_results_daily.start_date + 1,
    start_tod := '00:00'::time without time zone,
    end_tod := '24:00'::time without time zone,
    dow_list := ARRAY[extract('isodow' from cache_tt_results_daily.start_date)]::int[],
    node_start := cache_tt_results_daily.node_start,
    node_end := cache_tt_results_daily.node_end,
    holidays := True)
$BODY$;

COMMENT ON FUNCTION here_agg.cache_tt_results_daily
IS 'A simplified version of `cache_tt_results` for aggregating entire days of data.';
