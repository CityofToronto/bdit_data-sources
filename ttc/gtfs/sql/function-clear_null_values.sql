CREATE OR REPLACE FUNCTION gtfs.clear_null_values()
RETURNS void
SECURITY DEFINER
LANGUAGE sql
AS $$

    DELETE FROM gtfs.calendar_imp WHERE feed_id IS NULL;
    DELETE FROM gtfs.calendar_dates_imp WHERE feed_id IS NULL;
    DELETE FROM gtfs.routes WHERE feed_id IS NULL;
    DELETE FROM gtfs.shapes WHERE feed_id IS NULL;
    DELETE FROM gtfs.shapes_geom WHERE feed_id IS NULL;
    DELETE FROM gtfs.stop_times WHERE feed_id IS NULL;
    DELETE FROM gtfs.stops WHERE feed_id IS NULL;
    DELETE FROM gtfs.trips WHERE feed_id IS NULL;

$$;

ALTER FUNCTION gtfs.clear_null_values OWNER TO gtfs_admins;
GRANT EXECUTE ON FUNCTION gtfs.clear_null_values TO gtfs_bot;
