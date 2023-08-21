CREATE TABLE ecocounter.sites (
    site_id numeric PRIMARY KEY,
    site_description text NOT NULL,
    geom geometry(POINT, 4326),
    facility_description text,
    replaced_by numeric REFERENCES ecocounter.sites (site_id)
);

ALTER TABLE ecocounter.sites OWNER TO ecocounter_admins;

GRANT SELECT ON ecocounter.sites TO bdit_humans;

COMMENT ON COLUMN ecocounter.sites.site_id
IS 'sit identifier used by ecocounter';

COMMENT ON COLUMN ecocounter.sites.facility_description
IS 'description of bike-specific infrastructure which the sensor is installed within';

COMMENT ON COLUMN ecocounter.sites.replaced_by
IS 'Several sites had their sensors replaced and show up now as "new" sites though we should treat the data as continuous with the replaced site.';
