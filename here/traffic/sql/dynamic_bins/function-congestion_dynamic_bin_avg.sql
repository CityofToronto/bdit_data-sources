-- FUNCTION: here_agg.dynamic_bin_avg(date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean) --noqa: LT05

-- DROP FUNCTION IF EXISTS here_agg.dynamic_bin_avg(date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean); --noqa: LT05

CREATE OR REPLACE FUNCTION here_agg.dynamic_bin_avg(
    start_date date,
    end_date date,
    start_tod time without time zone,
    end_tod time without time zone,
    dow_list integer [],
    node_start bigint,
    node_end bigint,
    holidays boolean
)
RETURNS numeric
LANGUAGE plpgsql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

DECLARE uri_string_func text :=
    dynamic_bin_avg.node_start::text || '/' ||
    dynamic_bin_avg.node_end::text  || '/' ||
    dynamic_bin_avg.start_tod::text || '/' ||
    dynamic_bin_avg.end_tod::text || '/' ||
    dynamic_bin_avg.start_date::text || '/' ||
    dynamic_bin_avg.end_date::text || '/' ||
    dynamic_bin_avg.holidays::text || '/' ||
    dynamic_bin_avg.dow_list::text;
    res numeric;

BEGIN

--caches the dynamic binning results for this query
PERFORM here_agg.cache_tt_results(
    uri_string := uri_string_func,
    start_date := dynamic_bin_avg.start_date,
    end_date := dynamic_bin_avg.end_date,
    start_tod := dynamic_bin_avg.start_tod,
    end_tod := dynamic_bin_avg.end_tod,
    dow_list := dynamic_bin_avg.dow_list,
    node_start := dynamic_bin_avg.node_start,
    node_end := dynamic_bin_avg.node_end,
    holidays := dynamic_bin_avg.holidays
);

--the way we currently do it; find daily averages and then average.
WITH daily_means AS (
    SELECT
        dt_start::date,
        AVG(tt) AS daily_mean
    FROM here_agg.raw_corridors
    WHERE uri_string = uri_string_func
    GROUP BY dt_start::date
)

SELECT AVG(daily_mean) INTO res
FROM daily_means;

RETURN res;

END;

$BODY$;

ALTER FUNCTION here_agg.dynamic_bin_avg(
    date, date, time without time zone, time without time zone, integer [], bigint, bigint, boolean
)
OWNER TO here_admins;

COMMENT ON FUNCTION here_agg.dynamic_bin_avg IS
'Meant to mimic the TT app process; caches results for a specific request and 
then returns average TT.';
