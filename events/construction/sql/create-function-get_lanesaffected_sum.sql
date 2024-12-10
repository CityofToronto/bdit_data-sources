-- FUNCTION: itsc_factors.get_lanesaffected_sums(text)

-- DROP FUNCTION IF EXISTS itsc_factors.get_lanesaffected_sums(text);

CREATE OR REPLACE FUNCTION itsc_factors.get_lanesaffected_sums(
	input_string text)
    RETURNS TABLE (
        lane_open_auto integer,
        lane_closed_auto integer,
        lane_open_bike integer,
        lane_closed_bike integer,
        lane_open_ped integer,
        lane_closed_ped integer,
        lane_open_bus integer,
        lane_closed_bus integer
    )
    LANGUAGE plpgsql
    COST 100
    STABLE PARALLEL SAFE
    ROWS 1

AS $BODY$
DECLARE
    code_list TEXT[];
BEGIN

    -- Iterate over the list and aggregate sums for each code
    RETURN QUERY
    SELECT
        COALESCE(SUM(lane_open) FILTER (WHERE mode = 'Car'), 0)::int AS lane_open_auto,
        COALESCE(SUM(lane_closed) FILTER (WHERE mode = 'Car'), 0)::int AS lane_closed_auto,
        COALESCE(SUM(lane_open) FILTER (WHERE mode = 'Bike'), 0)::int AS lane_open_bike,
        COALESCE(SUM(lane_closed) FILTER (WHERE mode = 'Bike'), 0)::int AS lane_closed_bike,
        COALESCE(SUM(lane_open) FILTER (WHERE mode = 'Pedestrian'), 0)::int AS lane_open_ped,
        COALESCE(SUM(lane_closed) FILTER (WHERE mode = 'Pedestrian'), 0)::int AS lane_closed_ped,
        COALESCE(SUM(lane_open) FILTER (WHERE mode = 'Bus'), 0)::int AS lane_open_bus,
        COALESCE(SUM(lane_closed) FILTER (WHERE mode = 'Bus'), 0)::int AS lane_closed_bus
    FROM UNNEST(regexp_split_to_array(input_string, E'(?=(..)+$)')) AS c
    JOIN itsc_factors.lanesaffectedpattern AS lap ON lap.code = c;

END;
$BODY$;

ALTER FUNCTION itsc_factors.get_lanesaffected_sums(text) OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION itsc_factors.get_lanesaffected_sums(text) TO PUBLIC;

GRANT EXECUTE ON FUNCTION itsc_factors.get_lanesaffected_sums(text) TO congestion_admins;

GRANT EXECUTE ON FUNCTION itsc_factors.get_lanesaffected_sums(text) TO vds_bot;
