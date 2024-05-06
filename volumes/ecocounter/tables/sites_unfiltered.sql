CREATE TABLE ecocounter.sites (
    site_id numeric NOT NULL,
    site_description text COLLATE pg_catalog."default" NOT NULL,
    geom GEOMETRY (POINT, 4326),
    facility_description text COLLATE pg_catalog."default",
    notes text COLLATE pg_catalog."default",
    replaced_by_site_id numeric,
    validated boolean,
    CONSTRAINT sites_pkey PRIMARY KEY (site_id),
    CONSTRAINT sites_replaced_by_fkey FOREIGN KEY (replaced_by_site_id)
    REFERENCES ecocounter.sites_unfiltered (site_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE ecocounter.sites OWNER TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.sites FROM bdit_humans;
REVOKE ALL ON TABLE ecocounter.sites FROM ecocounter_bot;

GRANT ALL ON TABLE ecocounter.sites TO ecocounter_admins;
GRANT SELECT, INSERT ON TABLE ecocounter.sites TO ecocounter_bot;

COMMENT ON TABLE ecocounter.sites
IS 'CAUTION: Use VIEW `ecocounter.sites` which includes only sites verified by a human.
Sites or "locations" of separate ecocounter installations.
Each site may have one or more flows.';

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
