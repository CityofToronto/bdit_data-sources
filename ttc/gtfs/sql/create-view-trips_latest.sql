-- View: gtfs.trips_latest

-- DROP VIEW gtfs.trips_latest;

CREATE OR REPLACE VIEW gtfs.trips_latest
AS
SELECT
    trips.route_id,
    trips.service_id,
    trips.trip_id,
    trips.trip_headsign,
    trips.trip_short_name,
    trips.direction_id,
    trips.block_id,
    trips.shape_id,
    trips.wheelchair_accessible
FROM gtfs.trips
JOIN gtfs.calendar_latest USING (feed_id, service_id);

ALTER TABLE gtfs.trips_latest
OWNER TO gtfs_admins;

GRANT SELECT ON TABLE gtfs.trips_latest TO bdit_humans;
GRANT ALL ON TABLE gtfs.trips_latest TO gtfs_admins;

