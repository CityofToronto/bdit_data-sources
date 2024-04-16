CREATE TABLE ecocounter.sites (
    site_id numeric PRIMARY KEY,
    site_description text NOT NULL,
    geom GEOMETRY (POINT, 4326),
    facility_description text,
    notes text,
    replaced_by_site_id numeric REFERENCES ecocounter.sites (site_id)
);

ALTER TABLE ecocounter.sites OWNER TO ecocounter_admins;

GRANT SELECT ON ecocounter.sites TO bdit_humans;

COMMENT ON TABLE ecocounter.sites
IS 'Sites or "locations" of separate ecocounter
installations. Each site may have one or more flows.';

COMMENT ON COLUMN ecocounter.sites.site_id
IS 'unique site identifier used by ecocounter';

COMMENT ON COLUMN ecocounter.sites.facility_description
IS 'description of bike-specific infrastructure which the sensor is installed within';

COMMENT ON COLUMN ecocounter.sites.replaced_by_site_id
IS 'Several sites had their sensors replaced and
show up now as "new" sites though we should ideally
treat the data as continuous with the replaced site.
This field indicates the site_id of the new
replacement site, if any.';
