CREATE TABLE ecocounter.flows (
    flow_id numeric PRIMARY KEY,
    site_id numeric NOT NULL REFERENCES ecocounter.sites (site_id),
    flow_direction text NOT NULL,
    flow_geom geometry(LINESTRING,4326) CHECK(flow_geom IS NULL OR ST_NPoints(flow_geom) = 2),
    bin_size interval NOT NULL,
    replaced_by_flow_id numeric REFERENCES ecocounter.flows (flow_id),
    replaces_flow_id numeric REFERENCES ecocounter.flows (flow_id),
    directed boolean
);

GRANT SELECT ON ecocounter.flows TO bdit_humans;

ALTER TABLE ecocounter.flows OWNER TO ecocounter_admins;

COMMENT ON TABLE ecocounter.flows
IS 'A flow is usually a direction of travel associated with a sensor at an ecocounter installation site. For earlier sensors that did not detect directed flows, a flow may be both directions of travel together, i.e. just everyone who passed over the sensor any which way.';

COMMENT ON COLUMN ecocounter.flows.bin_size
IS 'temporal bins are either 15 or 30 minutes, depending on the sensor';

COMMENT ON COLUMN ecocounter.flows.flow_geom
IS 'a two-node line, where the first node indicates the position of the sensor and the second indicates the normal direction of travel over that sensor relative to the first node. I.e. the line segment is an arrow pointing in the direction of travel.';

COMMENT ON COLUMN ecocounter.flows.directed
IS 'indicates if the flow is strictly one-directional, ie. has a double sensor that separates the two directions of travel. Undirected flows will still have a primary direction of travel because of their location but would also count contraflow cyclists.';
