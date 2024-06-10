--Aggregate `vds.raw_vdsdata` into table `vds.counts_15min` by detector / 15min bins. (all lanes)
INSERT INTO vds.counts_15min (
    division_id, vdsconfig_uid, entity_location_uid, num_lanes, datetime_15min,
    count_15min, expected_bins, num_obs, num_distinct_lanes
)

/* Conversion of hourly volumes to count depends on size of bin.
These bin counts were determined by looking at the most common bin gap using:
bdit_data-sources/volumes/vds/exploration/time_gaps.sql */

SELECT
    d.division_id,
    d.vdsconfig_uid,
    d.entity_location_uid,
    c.lanes AS num_lanes,
    d.datetime_15min,
    SUM(d.volume_veh_per_hr) / 4 / di.expected_bins AS count_15min,
    -- / 4 to convert hourly volume to 15 minute volume
    -- / (expected_bins) to get average 15 minute volume depending on bin size
    -- (assumes blanks are 0)
    di.expected_bins,
    COUNT(*) AS num_obs,
    COUNT(DISTINCT d.lane) AS num_distinct_lanes
FROM vds.raw_vdsdata AS d
JOIN vds.vdsconfig AS c ON d.vdsconfig_uid = c.uid
JOIN vds.detector_inventory AS di
    ON di.vdsconfig_uid = d.vdsconfig_uid
    AND di.entity_location_uid = d.entity_location_uid
WHERE
    d.division_id = 2
    AND d.dt >= '{{ ds }} 00:00:00'::timestamp -- noqa: TMP
    AND d.dt < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY' -- noqa: TMP
    AND d.vdsconfig_uid IS NOT NULL
    AND d.entity_location_uid IS NOT NULL
GROUP BY
    d.division_id,
    d.vdsconfig_uid,
    d.entity_location_uid,
    c.lanes,
    di.expected_bins,
    d.datetime_15min
ON CONFLICT DO NOTHING;