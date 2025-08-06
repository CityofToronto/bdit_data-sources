-- Table: gtfs.routes

-- DROP TABLE IF EXISTS gtfs.routes;

CREATE TABLE IF NOT EXISTS gtfs.routes
(
    route_id integer NOT NULL,
    agency_id smallint NOT NULL,
    route_short_name text COLLATE pg_catalog."default" NOT NULL,
    route_long_name text COLLATE pg_catalog."default" NOT NULL,
    route_desc text COLLATE pg_catalog."default",
    route_type smallint NOT NULL,
    route_url text COLLATE pg_catalog."default",
    route_color character(6) COLLATE pg_catalog."default" NOT NULL,
    route_text_color character(6) COLLATE pg_catalog."default",
    feed_id integer,
    CONSTRAINT routes_route_feed_unique UNIQUE (route_id, feed_id),
    CONSTRAINT routes_feed_id_fkey FOREIGN KEY (feed_id)
    REFERENCES gtfs.feed_info (feed_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gtfs.routes
OWNER TO gtfs_admins;

REVOKE ALL ON TABLE gtfs.routes FROM bdit_humans;
REVOKE ALL ON TABLE gtfs.routes FROM gtfs_bot;

GRANT SELECT ON TABLE gtfs.routes TO bdit_humans;

GRANT ALL ON TABLE gtfs.routes TO gtfs_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE gtfs.routes TO gtfs_bot;
-- Index: fki_routes_feed_id_fkey

-- DROP INDEX IF EXISTS gtfs.fki_routes_feed_id_fkey;

CREATE INDEX IF NOT EXISTS fki_routes_feed_id_fkey
ON gtfs.routes USING btree
(feed_id ASC NULLS LAST)
TABLESPACE pg_default;
-- Index: routes_feed_id_route_id_idx

-- DROP INDEX IF EXISTS gtfs.routes_feed_id_route_id_idx;

CREATE INDEX IF NOT EXISTS routes_feed_id_route_id_idx
ON gtfs.routes USING btree
(feed_id ASC NULLS LAST, route_id ASC NULLS LAST)
TABLESPACE pg_default;