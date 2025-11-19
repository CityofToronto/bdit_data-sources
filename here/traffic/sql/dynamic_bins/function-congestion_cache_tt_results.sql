-- FUNCTION: gwolofs.congestion_cache_tt_results(text, date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean) --noqa: LT05

-- DROP FUNCTION IF EXISTS gwolofs.congestion_cache_tt_results(text, date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean); --noqa: LT05

CREATE OR REPLACE FUNCTION gwolofs.congestion_cache_tt_results(
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
    INSERT INTO gwolofs.congestion_raw_corridors (
        uri_string, dt, time_grp, corridor_id,  bin_range, tt, num_obs, hr
    )
    SELECT
        congestion_cache_tt_results.uri_string,
        dt, time_grp, corridor_id, bin_range, tt, num_obs, hr
    FROM gwolofs.congestion_return_dynamic_bins(
        congestion_cache_tt_results.start_date,
        congestion_cache_tt_results.end_date,
        congestion_cache_tt_results.start_tod,
        congestion_cache_tt_results.end_tod,
        congestion_cache_tt_results.dow_list,
        congestion_cache_tt_results.node_start,
        congestion_cache_tt_results.node_end,
        congestion_cache_tt_results.holidays
    )
    ON CONFLICT DO NOTHING;
    
$BODY$;

ALTER FUNCTION gwolofs.congestion_cache_tt_results(
    text, date, date, time without time zone,
    time without time zone, integer [], bigint, bigint, boolean
)
OWNER TO gwolofs;

COMMENT ON FUNCTION gwolofs.congestion_cache_tt_results IS
'Caches the dynamic binning results for a request.';

-- overload the function for more straightforward situation of daily corridor agg
CREATE OR REPLACE FUNCTION gwolofs.congestion_cache_tt_results_daily(
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
SELECT gwolofs.congestion_cache_tt_results(
    uri_string := NULL::text,
    start_date := congestion_cache_tt_results_daily.start_date,
    end_date := congestion_cache_tt_results_daily.start_date + 1,
    start_tod := '00:00'::time without time zone,
    end_tod := '24:00'::time without time zone,
    dow_list := ARRAY[extract('isodow' from congestion_cache_tt_results_daily.start_date)]::int[],
    node_start := congestion_cache_tt_results_daily.node_start,
    node_end := congestion_cache_tt_results_daily.node_end,
    holidays := True)
$BODY$;

COMMENT ON FUNCTION gwolofs.congestion_cache_tt_results_daily
IS 'A simplified version of `congestion_cache_tt_results` for aggregating entire days of data.';
