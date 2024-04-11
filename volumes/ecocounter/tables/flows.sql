CREATE TABLE ecocounter.flows (
    flow_id numeric NOT NULL,
    site_id numeric NOT NULL,
    flow_direction text COLLATE pg_catalog."default" NOT NULL,
    flow_geom GEOMETRY (LINESTRING, 4326),
    bin_size interval NOT NULL,
    notes text COLLATE pg_catalog."default",
    replaced_by_flow_id numeric,
    replaces_flow_id numeric,
    includes_contraflow boolean,
    validated boolean,
    CONSTRAINT locations_pkey PRIMARY KEY (flow_id),
    CONSTRAINT flows_replaced_by_flow_id_fkey FOREIGN KEY (replaced_by_flow_id)
    REFERENCES ecocounter.flows (flow_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION,
    CONSTRAINT flows_replaces_flow_id_fkey FOREIGN KEY (replaces_flow_id)
    REFERENCES ecocounter.flows (flow_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION,
    CONSTRAINT site_id_fk FOREIGN KEY (site_id)
    REFERENCES ecocounter.sites (site_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION,
    CONSTRAINT locations_flow_geom_check CHECK (flow_geom IS NULL OR st_npoints(flow_geom) = 2)
)
TABLESPACE pg_default;

ALTER TABLE ecocounter.flows OWNER TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.flows FROM aduyves;
REVOKE ALL ON TABLE ecocounter.flows FROM bdit_humans;
REVOKE ALL ON TABLE ecocounter.flows FROM ecocounter_bot;

GRANT UPDATE ON TABLE ecocounter.flows TO aduyves;

GRANT SELECT ON TABLE ecocounter.flows TO bdit_humans;

GRANT ALL ON TABLE ecocounter.flows TO ecocounter_admins;

GRANT SELECT, INSERT ON TABLE ecocounter.flows TO ecocounter_bot;

COMMENT ON TABLE ecocounter.flows
IS 'A flow is usually a direction of travel associated with a sensor at
an ecocounter installation site. For earlier sensors that did not detect
directed flows, a flow may be both directions of travel together, i.e.
just everyone who passed over the sensor any which way.

This table should only contain flows with at least some valid-ish data. ';

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

CREATE INDEX IF NOT EXISTS flows_flow_id_idx
ON ecocounter.flows USING btree (flow_id ASC NULLS LAST) 
TABLESPACE pg_default;