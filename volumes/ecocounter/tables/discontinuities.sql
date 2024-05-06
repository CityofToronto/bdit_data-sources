CREATE TABLE ecocounter.discontinuities (
    uid serial PRIMARY KEY,
    site_id numeric NOT NULL REFERENCES ecocounter.sites_unfiltered (site_id),
    -- moment the change takes place
    break timestamp NOT NULL,
    -- approximate bounds if the precise time is not known
    give_or_take interval,
    -- required description of what changed - be verbose!
    notes text NOT NULL
);

ALTER TABLE ecocounter.discontinuities OWNER TO ecocounter_admins;

GRANT SELECT ON ecocounter.discontinuities TO bdit_humans;

COMMENT ON TABLE ecocounter.discontinuities
IS 'Moments in time when data collection methods changed in such a way that we would expect clear pre- and post-change paradigms that may not be intercomparable.';
