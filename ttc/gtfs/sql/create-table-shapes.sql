-- Table: gtfs.shapes

-- DROP TABLE IF EXISTS gtfs.shapes;

CREATE TABLE IF NOT EXISTS gtfs.shapes
(
    shape_id bigint NOT NULL,
    shape_pt_lat text COLLATE pg_catalog."default" NOT NULL,
    shape_pt_lon text COLLATE pg_catalog."default" NOT NULL,
    shape_pt_sequence integer NOT NULL,
    shape_dist_traveled numeric(7, 4) DEFAULT 0,
    feed_id integer,
    CONSTRAINT shapes_shape_sequence_feed_unique UNIQUE (shape_id, shape_pt_sequence, feed_id),
    CONSTRAINT shapes_feed_id_fkey FOREIGN KEY (feed_id)
    REFERENCES gtfs.feed_info (feed_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gtfs.shapes
OWNER TO dbadmin;

REVOKE ALL ON TABLE gtfs.shapes FROM bdit_humans;
REVOKE ALL ON TABLE gtfs.shapes FROM gtfs_bot;

GRANT SELECT ON TABLE gtfs.shapes TO bdit_humans;

GRANT ALL ON TABLE gtfs.shapes TO dbadmin;

GRANT INSERT, SELECT, UPDATE ON TABLE gtfs.shapes TO gtfs_bot;
-- Index: fki_shapes_feed_id_fkey

-- DROP INDEX IF EXISTS gtfs.fki_shapes_feed_id_fkey;

CREATE INDEX IF NOT EXISTS fki_shapes_feed_id_fkey
ON gtfs.shapes USING btree
(feed_id ASC NULLS LAST)
TABLESPACE pg_default;

-- Trigger: create_shape_geom

-- DROP TRIGGER IF EXISTS create_shape_geom ON gtfs.shapes;

CREATE OR REPLACE TRIGGER create_shape_geom
AFTER INSERT
ON gtfs.shapes
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION gtfs.mk_shape_geom();