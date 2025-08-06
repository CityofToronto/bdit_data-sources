-- FUNCTION: gtfs.calendar_insert()

-- DROP FUNCTION IF EXISTS gtfs.calendar_insert();

CREATE OR REPLACE FUNCTION gtfs.calendar_insert()
RETURNS trigger
LANGUAGE plpgsql
COST 100
VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$

BEGIN
INSERT INTO gtfs.calendar_imp
VALUES (
NEW.service_id,
NEW.monday = 1,
NEW.tuesday = 1,
NEW.wednesday = 1,
NEW.thursday = 1,
NEW.friday = 1,
NEW.saturday = 1,
NEW.sunday = 1,
to_date(NEW.start_date::TEXT, 'YYYYMMDD'),
to_date(NEW.end_date::TEXT, 'YYYYMMDD'),
NEW.feed_id);
RETURN NULL;
END;
$BODY$;

ALTER FUNCTION gtfs.calendar_insert()
OWNER TO gtfs_admins;

GRANT EXECUTE ON FUNCTION gtfs.calendar_insert() TO gtfs_admins;

GRANT EXECUTE ON FUNCTION gtfs.calendar_insert() TO gtfs_bot;

REVOKE ALL ON FUNCTION gtfs.calendar_insert() FROM public;
