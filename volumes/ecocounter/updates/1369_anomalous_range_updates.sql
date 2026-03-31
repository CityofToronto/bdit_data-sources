--look at all the open ended anomalous ranges
SELECT anomalous_ranges.*, site_description
FROM ecocounter.anomalous_ranges
JOIN ecocounter.sites_unfiltered USING (site_id)
WHERE upper(time_range) IS NULL

UNION

SELECT anomalous_ranges.*, site_description
FROM ecocounter.anomalous_ranges
JOIN ecocounter.flows_unfiltered USING (flow_id)
JOIN ecocounter.sites_unfiltered ON flows_unfiltered.site_id = sites_unfiltered.site_id
WHERE upper(time_range) IS NULL
ORDER BY site_id

--look at validation results
SELECT site_id, ecocounter_site_description, direction_main, factor_jul2025, date_jul2025, last_active
FROM ecocounter.validation_summary WHERE factor_jul2025 IS NOT NULL
ORDER BY site_id

--delete two that were just validated 
DELETE FROM ecocounter.anomalous_ranges WHERE site_id = 300028396 AND uid = 87; --bloor and Huron
DELETE FROM ecocounter.anomalous_ranges WHERE flow_id = 353341347 AND uid = 86; --yonge and st clair

--Make the old Bloor and Huron AR not open-ended anymore.
UPDATE ecocounter.anomalous_ranges SET time_range = tsrange('2021-02-09'::date, '2022-06-03'::date, '[)') WHERE uid = 2 AND site_id = 100042942;
