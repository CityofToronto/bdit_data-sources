CREATE TABLE ecocounter.sites (
    site_id numeric PRIMARY KEY,
    site_description text NOT NULL,
    geom geometry(POINT, 4326)
);

GRANT SELECT ON ecocounter.sites TO bdit_humans;
