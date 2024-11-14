--DROP VIEW ecocounter.open_data_sites;
CREATE VIEW ecocounter.open_data_sites AS
SELECT
    s.site_id,
    s.site_description,
    s.geom,
    s.facility_description,
    s.notes AS site_notes,
    s.replaced_by_site_id,
    s.centreline_id,
    s.first_active,
    s.date_decommissioned,
    f.flow_id,
    f.flow_direction,
    f.direction_main,
    f.flow_geom,
    f.bin_size
FROM ecocounter.sites AS s
JOIN ecocounter.flows AS f USING (site_id);

ALTER VIEW ecocounter.open_data_sites OWNER TO ecocounter_admins;
GRANT SELECT ON VIEW ecocounter.open_data_sites TO ecocounter_bot;
GRANT SELECT ON VIEW ecocounter.open_data_sites TO bdit_humans;
