-- FUNCTION: gwolofs.congestion_dynamic_bin_avg(date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean)

-- DROP FUNCTION IF EXISTS gwolofs.congestion_dynamic_bin_avg(date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean);

CREATE OR REPLACE FUNCTION gwolofs.congestion_dynamic_bin_avg(
    start_date date,
    end_date date,
    start_tod time without time zone,
    end_tod time without time zone,
    dow_list integer[],
    node_start bigint,
    node_end bigint,
    holidays boolean)
    RETURNS numeric
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

DECLARE uri_string_func text :=
    here_dynamic_bin_avg.node_start::text || '/' ||
    here_dynamic_bin_avg.node_end::text  || '/' ||
    here_dynamic_bin_avg.start_tod::text || '/' ||
    here_dynamic_bin_avg.end_tod::text || '/' ||
    here_dynamic_bin_avg.start_date::text || '/' ||
    here_dynamic_bin_avg.end_date::text || '/' ||
    here_dynamic_bin_avg.holidays::text || '/' ||
    here_dynamic_bin_avg.dow_list::text;
    res numeric;

BEGIN

PERFORM gwolofs.congestion_cache_tt_results(
    uri_string := uri_string_func,
    start_date := here_dynamic_bin_avg.start_date,
    end_date := here_dynamic_bin_avg.end_date,
    start_tod := here_dynamic_bin_avg.start_tod,
    end_tod := here_dynamic_bin_avg.end_tod,
    dow_list := here_dynamic_bin_avg.dow_list,
    node_start := here_dynamic_bin_avg.node_start,
    node_end := here_dynamic_bin_avg.node_end,
    holidays := here_dynamic_bin_avg.holidays
);

WITH daily_means AS (
    SELECT
        dt_start::date,
        AVG(tt) AS daily_mean
    FROM gwolofs.congestion_raw_corridors
    WHERE uri_string = uri_string_func
    GROUP BY dt_start::date
)

SELECT AVG(daily_mean) INTO res
FROM daily_means;

RETURN res;

END;

$BODY$;

ALTER FUNCTION gwolofs.congestion_dynamic_bin_avg(date, date, time without time zone, time without time zone, integer[], bigint, bigint, boolean)
    OWNER TO gwolofs;

COMMENT ON FUNCTION gwolofs.congestion_dynamic_bin_avg IS 'Previously gwolofs.here_dynamic_bin_avg.';
