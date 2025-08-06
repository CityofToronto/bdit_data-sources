-- Table: gtfs.shapes_geom

-- DROP TABLE IF EXISTS gtfs.shapes_geom;

CREATE TABLE IF NOT EXISTS gtfs.shapes_geom
(
    shape_id bigint NOT NULL,
    feed_id integer,
    geom GEOMETRY (LINESTRING, 4326),
    CONSTRAINT shapes_geom_feed_unique UNIQUE (shape_id, feed_id),
    CONSTRAINT shapes_geom_feed_id_fkey FOREIGN KEY (feed_id)
    REFERENCES gtfs.feed_info (feed_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gtfs.shapes_geom
OWNER TO gtfs_admins;

REVOKE ALL ON TABLE gtfs.shapes_geom FROM bdit_humans;
REVOKE ALL ON TABLE gtfs.shapes_geom FROM gtfs_bot;

GRANT SELECT ON TABLE gtfs.shapes_geom TO bdit_humans;

GRANT ALL ON TABLE gtfs.shapes_geom TO gtfs_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE gtfs.shapes_geom TO gtfs_bot;
-- Index: fki_shapes_geom_feed_id_fkey

-- DROP INDEX IF EXISTS gtfs.fki_shapes_geom_feed_id_fkey;

CREATE INDEX IF NOT EXISTS fki_shapes_geom_feed_id_fkey
ON gtfs.shapes_geom USING btree
(feed_id ASC NULLS LAST)
TABLESPACE pg_default;
-- Index: shapes_geom_feed_id_shape_id_idx

-- DROP INDEX IF EXISTS gtfs.shapes_geom_feed_id_shape_id_idx;

CREATE INDEX IF NOT EXISTS shapes_geom_feed_id_shape_id_idx
ON gtfs.shapes_geom USING btree
(feed_id ASC NULLS LAST, shape_id ASC NULLS LAST)
TABLESPACE pg_default;