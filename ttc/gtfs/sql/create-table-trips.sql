-- Table: gtfs.trips

-- DROP TABLE IF EXISTS gtfs.trips;

CREATE TABLE IF NOT EXISTS gtfs.trips
(
    route_id integer NOT NULL,
    service_id smallint NOT NULL,
    trip_id bigint NOT NULL,
    trip_headsign text COLLATE pg_catalog."default",
    trip_short_name text COLLATE pg_catalog."default",
    direction_id smallint NOT NULL,
    block_id bigint,
    shape_id integer NOT NULL,
    wheelchair_accessible smallint,
    feed_id integer,
    bikes_allowed integer,
    CONSTRAINT trips_feed_unique UNIQUE (trip_id, feed_id),
    CONSTRAINT trips_feed_id_fkey FOREIGN KEY (feed_id)
    REFERENCES gtfs.feed_info (feed_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gtfs.trips
OWNER TO gtfs_admins;

REVOKE ALL ON TABLE gtfs.trips FROM bdit_humans;
REVOKE ALL ON TABLE gtfs.trips FROM gtfs_bot;

GRANT SELECT ON TABLE gtfs.trips TO bdit_humans;

GRANT ALL ON TABLE gtfs.trips TO gtfs_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE gtfs.trips TO gtfs_bot;
-- Index: fki_trips_feed_id_fkey

-- DROP INDEX IF EXISTS gtfs.fki_trips_feed_id_fkey;

CREATE INDEX IF NOT EXISTS fki_trips_feed_id_fkey
ON gtfs.trips USING btree
(feed_id ASC NULLS LAST)
TABLESPACE pg_default;
-- Index: trips_feed_id_route_id_idx

-- DROP INDEX IF EXISTS gtfs.trips_feed_id_route_id_idx;

CREATE INDEX IF NOT EXISTS trips_feed_id_route_id_idx
ON gtfs.trips USING btree
(feed_id ASC NULLS LAST, route_id ASC NULLS LAST)
TABLESPACE pg_default;
-- Index: trips_feed_id_shape_id_idx

-- DROP INDEX IF EXISTS gtfs.trips_feed_id_shape_id_idx;

CREATE INDEX IF NOT EXISTS trips_feed_id_shape_id_idx
ON gtfs.trips USING btree
(feed_id ASC NULLS LAST, shape_id ASC NULLS LAST)
TABLESPACE pg_default;