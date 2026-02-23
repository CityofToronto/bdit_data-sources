--aggregate `vds.raw_vdsdata` into `vds.counts_15min_bylane` table by detector / lane / 15min bins.'
INSERT INTO vds.counts_15min_bylane (
    division_id, vdsconfig_uid, entity_location_uid, lane, datetime_15min,
    count_15min, expected_bins, num_obs
)

/* Conversion of hourly volumes to count depends on size of bin.
These bin counts were determined by looking at the most common bin gap using:
bdit_data-sources/volumes/vds/exploration/time_gaps.sql */
SELECT
    d.division_id,
    d.vdsconfig_uid,
    d.entity_location_uid,
    d.lane,
    d.datetime_15min,
    SUM(d.volume_veh_per_hr) / 4 / di.expected_bins AS count_15min,
    -- / 4 to convert hourly volume to 15 minute volume
    -- / (expected_bins) to get average 15 minute volume depending on 
    -- bin size (assumes blanks are 0)
    di.expected_bins,
    COUNT(*) AS num_obs
FROM vds.raw_vdsdata AS d
JOIN vds.detector_inventory AS di USING (vdsconfig_uid, entity_location_uid)
WHERE
    --division 8001 sensors have only 1 lane, aggregate only into view counts_15min_div8001.
    d.division_id = 2
    AND d.dt >= '{{ ds }} 00:00:00'::timestamp -- noqa: TMP
    AND d.dt < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY' -- noqa: TMP
    AND d.vdsconfig_uid IS NOT NULL
    AND d.entity_location_uid IS NOT NULL
GROUP BY
    d.division_id,
    d.vdsconfig_uid,
    d.entity_location_uid,
    di.expected_bins,
    d.lane,
    d.datetime_15min
ON CONFLICT DO NOTHING;