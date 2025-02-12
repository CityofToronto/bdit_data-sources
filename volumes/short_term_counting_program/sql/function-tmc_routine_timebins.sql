CREATE OR REPLACE FUNCTION traffic.tmc_routine_timebins(
    dt date
)
RETURNS timestamp without time zone []
LANGUAGE 'sql'
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$
SELECT ARRAY_AGG(dt_bins)
FROM (VALUES
    ('7:30'::time, '9:30'::time),
    ('10:00', '12:00'),
    ('13:00', '15:00'),
    ('16:00', '18:00')
) AS time_ranges(start_tod, end_tod),
generate_series(
    tmc_routine_timebins.dt + time_ranges.start_tod,
    tmc_routine_timebins.dt + time_ranges.end_tod - '15 minutes'::interval,
    '15 minutes'::interval
) AS dt_bins
$BODY$;

ALTER FUNCTION traffic.tmc_routine_timebins(date)
OWNER TO traffic_admins;

GRANT EXECUTE ON FUNCTION traffic.tmc_routine_timebins(date)
TO bdit_humans;

COMMENT ON FUNCTION traffic.tmc_routine_timebins(date) IS
'Returns the 15minute bins associated with an 8R count on the input date. '
'Useful for comparing 14Hr counts with 8R counts.';
