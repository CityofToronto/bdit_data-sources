CREATE TABLE ecocounter.flows (
    flow_id numeric PRIMARY KEY,
    site_id numeric NOT NULL,
    flow_direction text NOT NULL,
    flow_geom geometry(LINESTRING,4326) CHECK(flow_geom IS NULL OR ST_NPoints(flow_geom) = 2),
    bin_size interval NOT NULL
);

GRANT SELECT ON ecocounter.flows TO bdit_humans;

COMMENT ON COLUMN ecocounter.flows.bin_size
IS 'temporal bins are either 15 or 30 minutes, depending on the sensor';

COMMENT ON COLUMN ecocounter.flows.flow_geom
IS 'a two-node line, where the first node indicates the position of the sensor and the second indicates the normal direction of travel over that sensor relative to the first node. I.e. the line segment is an arrow pointing in the direction of travel.';
