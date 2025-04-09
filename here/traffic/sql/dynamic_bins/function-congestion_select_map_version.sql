-- FUNCTION: gwolofs.congestion_select_map_version(date, date)

-- DROP FUNCTION IF EXISTS gwolofs.congestion_select_map_version(date, date);

CREATE OR REPLACE FUNCTION gwolofs.congestion_select_map_version(
    start_date date,
    end_date date
)
RETURNS text
LANGUAGE sql
COST 100
STABLE PARALLEL SAFE
AS $BODY$

SELECT street_version
FROM here.street_valid_range AS svr,
LATERAL (
    SELECT svr.valid_range * daterange(
        congestion_select_map_version.start_date,
        congestion_select_map_version.end_date, '[)') AS overlap
) AS lat
WHERE UPPER(lat.overlap) - LOWER(lat.overlap) IS NOT NULL
ORDER BY UPPER(lat.overlap) - LOWER(lat.overlap) DESC NULLS LAST
LIMIT 1;

$BODY$;

ALTER FUNCTION gwolofs.congestion_select_map_version(date, date)
OWNER TO gwolofs;

GRANT EXECUTE ON FUNCTION gwolofs.congestion_select_map_version(date, date) TO congestion_bot;

COMMENT ON FUNCTION gwolofs.congestion_select_map_version IS
'Implement TT App selectMapVersion.py';
