CREATE VIEW ecocounter.sites AS (
    SELECT
        site_id,
        site_description,
        geom,
        facility_description,
        notes,
        replaced_by_site_id
    FROM ecocounter.sites_unfiltered
    WHERE validated
);

ALTER VIEW ecocounter.sites OWNER TO ecocounter_admins;
GRANT ALL ON TABLE ecocounter.sites TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.sites FROM bdit_humans;
GRANT SELECT ON TABLE ecocounter.sites TO bdit_humans;

GRANT SELECT ON TABLE ecocounter.sites TO ecocounter_bot;

COMMENT ON VIEW ecocounter.sites
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