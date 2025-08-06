-- Table: gtfs.stops

-- DROP TABLE IF EXISTS gtfs.stops;

CREATE TABLE IF NOT EXISTS gtfs.stops
(
    stop_id integer NOT NULL,
    stop_code text COLLATE pg_catalog."default" NOT NULL,
    stop_name text COLLATE pg_catalog."default" NOT NULL,
    stop_desc text COLLATE pg_catalog."default",
    stop_lat text COLLATE pg_catalog."default" NOT NULL,
    stop_lon text COLLATE pg_catalog."default" NOT NULL,
    geom GEOMETRY (POINT, 4326),
    zone_id smallint,
    stop_url text COLLATE pg_catalog."default",
    location_type text COLLATE pg_catalog."default",
    parent_station integer,
    wheelchair_boarding smallint,
    feed_id integer,
    stop_timezone text COLLATE pg_catalog."default",
    CONSTRAINT stops_feed_unique UNIQUE (stop_id, feed_id),
    CONSTRAINT stops_feed_id_fkey FOREIGN KEY (feed_id)
    REFERENCES gtfs.feed_info (feed_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gtfs.stops
OWNER TO gtfs_admins;

REVOKE ALL ON TABLE gtfs.stops FROM bdit_humans;
REVOKE ALL ON TABLE gtfs.stops FROM gtfs_bot;

GRANT SELECT ON TABLE gtfs.stops TO bdit_humans;

GRANT ALL ON TABLE gtfs.stops TO gtfs_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE gtfs.stops TO gtfs_bot;
-- Index: fki_stops_feed_id_fkey

-- DROP INDEX IF EXISTS gtfs.fki_stops_feed_id_fkey;

CREATE INDEX IF NOT EXISTS fki_stops_feed_id_fkey
ON gtfs.stops USING btree
(feed_id ASC NULLS LAST)
TABLESPACE pg_default;

-- Trigger: create_geom

-- DROP TRIGGER IF EXISTS create_geom ON gtfs.stops;

CREATE OR REPLACE TRIGGER create_geom
BEFORE INSERT
ON gtfs.stops
FOR EACH ROW
EXECUTE FUNCTION gtfs.trg_mk_geom();