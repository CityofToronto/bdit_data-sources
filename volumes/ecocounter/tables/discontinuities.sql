CREATE TABLE ecocounter.discontinuities (
    uid serial PRIMARY KEY,
    -- a references to EITHER a site_id or flow_id
    site_id numeric REFERENCES ecocounter.sites (site_id),
    flow_id numeric REFERENCES ecocounter.flows (flow_id),
    -- moment the change takes place
    break timestamp NOT NULL,
    -- approximate bounds if the precise time is not known
    give_or_take interval,
    -- required description of what changed - be verbose!
    notes text NOT NULL
    CHECK (
        -- only one or the other specified but not both
        (site_id IS NOT NULL OR flow_id IS NOT NULL)
        AND NOT (site_id IS NOT NULL AND flow_id IS NOT NULL)
    )
);

ALTER TABLE ecocounter.discontinuities OWNER TO ecocounter_admins;

GRANT SELECT ON ecocounter.discontinuities TO bdit_humans;