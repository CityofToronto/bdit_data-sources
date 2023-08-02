CREATE TABLE ecocounter.sites (
    site_id numeric PRIMARY KEY,
    site_description text NOT NULL,
    geom geometry(POINT, 4326),
    facility_description text
);

ALTER TABLE ecocounter.sites OWNER TO ecocounter_admins;

GRANT SELECT ON ecocounter.sites TO bdit_humans;

COMMENT ON COLUMN ecocounter.sites.facility_description
IS 'description of bike-specific infrastructure which the sensor is installed within';
