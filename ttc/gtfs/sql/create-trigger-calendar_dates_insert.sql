-- FUNCTION: gtfs.calendar_dates_insert()

-- DROP FUNCTION IF EXISTS gtfs.calendar_dates_insert();

CREATE OR REPLACE FUNCTION gtfs.calendar_dates_insert()
RETURNS trigger
LANGUAGE plpgsql
COST 100
VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$

BEGIN
INSERT INTO gtfs.calendar_dates_imp
VALUES(NEW.service_id, to_date(NEW.date_::TEXT, 'YYYYMMDD'), NEW.exception_type, NEW.feed_id);
RETURN NULL;
END;
$BODY$;

ALTER FUNCTION gtfs.calendar_dates_insert()
OWNER TO gtfs_admins;

GRANT EXECUTE ON FUNCTION gtfs.calendar_dates_insert() TO gtfs_admins;

GRANT EXECUTE ON FUNCTION gtfs.calendar_dates_insert() TO gtfs_bot;

REVOKE ALL ON FUNCTION gtfs.calendar_dates_insert() FROM public;
