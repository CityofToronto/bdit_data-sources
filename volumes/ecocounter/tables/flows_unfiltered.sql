CREATE TABLE ecocounter.flows_unfiltered (
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
    first_active timestamp without time zone,
    last_active timestamp without time zone,
    date_decommissioned timestamp without time zone,
    direction_main travel_directions,
    mode_counted text, -- whether flow counts bikes or scooters or...
    CONSTRAINT locations_pkey PRIMARY KEY (flow_id),
    CONSTRAINT flows_replaced_by_flow_id_fkey FOREIGN KEY (replaced_by_flow_id)
    REFERENCES ecocounter.flows_unfiltered (flow_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION,
    CONSTRAINT flows_replaces_flow_id_fkey FOREIGN KEY (replaces_flow_id)
    REFERENCES ecocounter.flows_unfiltered (flow_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION,
    CONSTRAINT site_id_fk FOREIGN KEY (site_id)
    REFERENCES ecocounter.sites_unfiltered (site_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION,
    CONSTRAINT locations_flow_geom_check CHECK (flow_geom IS NULL OR st_npoints(flow_geom) = 2)
)
TABLESPACE pg_default;

ALTER TABLE ecocounter.flows_unfiltered OWNER TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.flows_unfiltered FROM aduyves;
REVOKE ALL ON TABLE ecocounter.flows_unfiltered FROM bdit_humans;
REVOKE ALL ON TABLE ecocounter.flows_unfiltered FROM ecocounter_bot;

GRANT ALL ON TABLE ecocounter.flows_unfiltered TO ecocounter_admins;
GRANT SELECT, INSERT ON TABLE ecocounter.flows_unfiltered TO ecocounter_bot;

COMMENT ON TABLE ecocounter.flows_unfiltered IS E''
'CAUTION: Use VIEW `ecocounter.flows` which includes only flows verified by a human. '
'A flow is usually a direction of travel associated with a sensor at '
'an ecocounter installation site. For earlier sensors that did not detect '
'directed flows, a flow may be both directions of travel together, i.e. '
'just everyone who passed over the sensor any which way.';

COMMENT ON COLUMN ecocounter.flows_unfiltered.bin_size
IS 'temporal bins are either 15, 30, or 60 minutes, depending on the sensor';

COMMENT ON COLUMN ecocounter.flows_unfiltered.flow_geom IS E''
'A two-node line, where the first node indicates the position of the sensor and '
'the second indicates the normal direction of travel over that sensor relative to the '
'first node. I.e. the line segment is an arrow pointing in the direction of travel.';

COMMENT ON COLUMN ecocounter.flows_unfiltered.includes_contraflow IS E''
'Does the flow also count travel in the reverse of the indicated flow direction? '
'TRUE indicates that the flow, though installed in one-way infrastucture like a standard bike '
'lane, also counts travel going the wrong direction within that lane.';

COMMENT ON COLUMN ecocounter.flows_unfiltered.first_active IS E''
'First timestamp flow_id appears in ecocounter.counts_unfiltered. '
'Updated using trigger with each insert on ecocounter.counts_unfiltered. ';

COMMENT ON COLUMN ecocounter.flows_unfiltered.last_active IS E''
'Last timestamp flow_id appears in ecocounter.counts_unfiltered. '
'Updated using trigger with each insert on ecocounter.counts_unfiltered. ';

CREATE INDEX IF NOT EXISTS flows_flow_id_idx
ON ecocounter.flows_unfiltered USING btree (flow_id ASC NULLS LAST)
TABLESPACE pg_default;

CREATE TYPE travel_directions AS ENUM ('Northbound', 'Southbound', 'Westbound', 'Eastbound');