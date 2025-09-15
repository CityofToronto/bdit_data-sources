-- FUNCTION: gwolofs.congestion_select_map_version(date, date)

-- DROP FUNCTION IF EXISTS gwolofs.congestion_select_map_version(date, date);

CREATE OR REPLACE FUNCTION gwolofs.congestion_select_map_version(
    start_date date,
    end_date date,
    agg_type text DEFAULT NULL, --null or 'path'
    OUT selected_version text
)
RETURNS text
LANGUAGE plpgsql
COST 100
STABLE PARALLEL SAFE
AS $BODY$

DECLARE
    svr text := 'street_valid_range' || CASE agg_type WHEN 'path' THEN '_path' ELSE '' END;

BEGIN
EXECUTE FORMAT(
    $$
    SELECT street_version
    FROM here.%I AS svr,
    LATERAL (
        SELECT svr.valid_range * daterange(%L, %L, '[)') AS overlap
    ) AS lat
    WHERE UPPER(lat.overlap) - LOWER(lat.overlap) IS NOT NULL
    ORDER BY UPPER(lat.overlap) - LOWER(lat.overlap) DESC NULLS LAST
    LIMIT 1;
    $$, svr, congestion_select_map_version.start_date, congestion_select_map_version.end_date
) INTO selected_version;
END;
$BODY$;

ALTER FUNCTION gwolofs.congestion_select_map_version(date, date, text)
OWNER TO gwolofs;

GRANT EXECUTE ON FUNCTION gwolofs.congestion_select_map_version(date, date, text) TO congestion_bot;

COMMENT ON FUNCTION gwolofs.congestion_select_map_version IS
'Implement TT App selectMapVersion.py';

--test cases
SELECT * FROM gwolofs.congestion_select_map_version('2022-01-01', '2023-01-01');
SELECT * FROM gwolofs.congestion_select_map_version('2022-01-01', '2023-01-01', 'path');