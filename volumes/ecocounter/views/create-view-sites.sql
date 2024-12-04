CREATE OR REPLACE VIEW ecocounter.sites AS (
    SELECT
        site_id,
        site_description,
        geom,
        facility_description,
        notes,
        replaced_by_site_id,
        centreline_id,
        first_active,
        last_active,
        date_decommissioned,
        counter,
        linear_name_full,
        side_street,
        technology
    FROM ecocounter.sites_unfiltered
    WHERE validated
);

ALTER VIEW ecocounter.sites OWNER TO ecocounter_admins;
GRANT ALL ON TABLE ecocounter.sites TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.sites FROM bdit_humans;
GRANT SELECT ON TABLE ecocounter.sites TO bdit_humans;

GRANT SELECT ON TABLE ecocounter.sites TO ecocounter_bot;

COMMENT ON VIEW ecocounter.sites IS E''
'CAUTION: Use VIEW `ecocounter.sites` which includes only sites verified by a human.'
'Sites represent separate installations, where each site may have one or more flow.';

COMMENT ON COLUMN ecocounter.sites.site_id
IS 'unique site identifier used by ecocounter';

COMMENT ON COLUMN ecocounter.sites.facility_description
IS 'description of bike-specific infrastructure which the sensor is installed within';

COMMENT ON COLUMN ecocounter.sites.replaced_by_site_id IS E''
'Several sites had their sensors replaced and show up now as "new" sites though we should '
'ideally treat the data as continuous with the replaced site. This field indicates the site_id '
'of the new replacement site, if any.';

COMMENT ON COLUMN ecocounter.sites.centreline_id IS E''
'The nearest street centreline_id, noting that ecocounter sensors are only configured to count '
'bike-like objects on a portion of the roadway ie. cycletrack or multi-use-path. '
'Join using `JOIN gis_core.centreline_latest USING (centreline_id)`.';

COMMENT ON COLUMN ecocounter.sites.first_active IS E''
'First timestamp site_id appears in ecocounter.counts_unfiltered. '
'Updated using trigger with each insert on ecocounter.counts_unfiltered. ';

COMMENT ON COLUMN ecocounter.sites.last_active IS E''
'Last timestamp site_id appears in ecocounter.counts_unfiltered. '
'Updated using trigger with each insert on ecocounter.counts_unfiltered. ';
