CREATE TABLE ecocounter.site_counts (
    site_id numeric REFERENCES ecocounter.sites (site_id),
    datetime_bin timestamp NOT NULL,
    volume smallint,
    UNIQUE(site_id, datetime_bin)
);

CREATE INDEX ON ecocounter.site_counts (datetime_bin);

GRANT SELECT ON ecocounter.site_counts TO bdit_humans;