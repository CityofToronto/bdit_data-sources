--create a view similar to `vds.counts_15min` for division_id = 8001.
--use view instead of table because unlike division_id = 2 this data 
--is already at 15 minutes in raw format.

--DROP VIEW vds.counts_15min_div8001;
CREATE VIEW vds.counts_15min_div8001 AS (
    SELECT
        d.division_id,
        d.vdsconfig_uid,
        d.entity_location_uid,
        c.lanes AS num_lanes,
        d.datetime_15min,
        SUM(d.volume_veh_per_hr) / 4 / di.expected_bins AS count_15min,
        di.expected_bins,
        COUNT(*) AS num_obs,
        COUNT(DISTINCT d.lane) AS num_distinct_lanes
    FROM vds.raw_vdsdata AS d
    JOIN vds.detector_inventory AS di USING (vdsconfig_uid, entity_location_uid)
    LEFT JOIN vds.vdsconfig AS c ON d.vdsconfig_uid = c.uid
    WHERE d.division_id = 8001
    GROUP BY
        d.division_id,
        d.vdsconfig_uid,
        d.entity_location_uid,
        c.lanes,
        di.expected_bins,
        d.datetime_15min
);

ALTER VIEW vds.counts_15min_div8001 OWNER TO vds_admins;
GRANT SELECT ON vds.counts_15min_div8001 TO bdit_humans, vds_bot;