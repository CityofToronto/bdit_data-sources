-- View: gtfs.routes_latest

-- DROP VIEW gtfs.routes_latest;

CREATE OR REPLACE VIEW gtfs.routes_latest
AS
SELECT
    routes.route_id,
    routes.agency_id,
    routes.route_short_name,
    routes.route_long_name,
    routes.route_desc,
    routes.route_type,
    routes.route_url,
    routes.route_color,
    routes.route_text_color
FROM gtfs.routes
JOIN gtfs.calendar_latest USING (feed_id);

ALTER TABLE gtfs.routes_latest
OWNER TO gtfs_admins;

GRANT SELECT ON TABLE gtfs.routes_latest TO bdit_humans;
GRANT ALL ON TABLE gtfs.routes_latest TO gtfs_admins;

