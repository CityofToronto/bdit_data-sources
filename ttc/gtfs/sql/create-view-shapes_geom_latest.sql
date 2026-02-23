-- View: gtfs.shapes_geom_latest

-- DROP VIEW gtfs.shapes_geom_latest;

CREATE OR REPLACE VIEW gtfs.shapes_geom_latest
AS
SELECT
    shapes_geom.shape_id,
    shapes_geom.feed_id,
    shapes_geom.geom
FROM gtfs.shapes_geom
JOIN gtfs.calendar_latest USING (feed_id);

ALTER TABLE gtfs.shapes_geom_latest
OWNER TO gtfs_admins;

GRANT SELECT ON TABLE gtfs.shapes_geom_latest TO bdit_humans;
GRANT ALL ON TABLE gtfs.shapes_geom_latest TO gtfs_admins;

