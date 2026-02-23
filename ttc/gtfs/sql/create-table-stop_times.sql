-- Table: gtfs.stop_times

-- DROP TABLE IF EXISTS gtfs.stop_times;

CREATE TABLE IF NOT EXISTS gtfs.stop_times
(
    trip_id bigint NOT NULL,
    arrival_time interval NOT NULL,
    departure_time interval NOT NULL,
    stop_id integer NOT NULL,
    stop_sequence smallint NOT NULL,
    stop_headsign text COLLATE pg_catalog."default",
    pickup_type smallint NOT NULL,
    drop_off_type smallint NOT NULL,
    shape_dist_traveled numeric(7, 4) DEFAULT 0,
    feed_id integer,
    CONSTRAINT stop_times_trip_feed_unique UNIQUE (trip_id, feed_id, stop_sequence),
    CONSTRAINT stop_times_feed_id_fkey FOREIGN KEY (feed_id)
    REFERENCES gtfs.feed_info (feed_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gtfs.stop_times
OWNER TO gtfs_admins;

REVOKE ALL ON TABLE gtfs.stop_times FROM bdit_humans;
REVOKE ALL ON TABLE gtfs.stop_times FROM gtfs_bot;

GRANT SELECT ON TABLE gtfs.stop_times TO bdit_humans;

GRANT ALL ON TABLE gtfs.stop_times TO gtfs_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE gtfs.stop_times TO gtfs_bot;
-- Index: fki_stop_times_feed_id_fkey

-- DROP INDEX IF EXISTS gtfs.fki_stop_times_feed_id_fkey;

CREATE INDEX IF NOT EXISTS fki_stop_times_feed_id_fkey
ON gtfs.stop_times USING btree
(feed_id ASC NULLS LAST)
TABLESPACE pg_default;