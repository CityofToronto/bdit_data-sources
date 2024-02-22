CREATE TABLE ecocounter.flows (
    flow_id numeric PRIMARY KEY,
    site_id numeric NOT NULL REFERENCES ecocounter.sites (site_id),
    flow_direction text NOT NULL,
    flow_geom GEOMETRY (LINESTRING, 4326) CHECK (flow_geom IS NULL OR ST_NPoints(flow_geom) = 2),
    bin_size interval NOT NULL,
    notes text,
    replaced_by_flow_id numeric REFERENCES ecocounter.flows (flow_id),
    replaces_flow_id numeric REFERENCES ecocounter.flows (flow_id),
    includes_contraflow boolean
);

GRANT SELECT ON ecocounter.flows TO bdit_humans;

ALTER TABLE ecocounter.flows OWNER TO ecocounter_admins;

COMMENT ON TABLE ecocounter.flows
IS 'A flow is usually a direction of travel
associated with a sensor at an ecocounter
installation site. For earlier sensors that
did not detect directed flows, a flow may be
both directions of travel together,
i.e. just everyone who passed over the sensor
any which way.';

COMMENT ON COLUMN ecocounter.flows.bin_size
IS 'temporal bins are either 15, 30, or 60 minutes, depending on the sensor';

COMMENT ON COLUMN ecocounter.flows.flow_geom
IS 'A two-node line, where the first node 
indicates the position of the sensor and
the second indicates the normal direction
of travel over that sensor relative to the
first node. I.e. the line segment is an
arrow pointing in the direction of travel.';

COMMENT ON COLUMN ecocounter.flows.includes_contraflow
IS 'Does the flow also count travel in the reverse of
the indicated flow direction?
TRUE indicates that the flow, though installed
in one-way infrastucture like a standard bike lane,
also counts travel going the wrong direction within
that lane.';
