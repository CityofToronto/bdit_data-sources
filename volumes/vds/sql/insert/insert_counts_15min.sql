--Aggregate `vds.raw_vdsdata` into table `vds.counts_15min` by detector / 15min bins. (all lanes)
INSERT INTO vds.counts_15min (division_id, vdsconfig_uid, entity_location_uid, num_lanes, datetime_15min,
    count_15min, expected_bins, num_obs, num_distinct_lanes)

/* Conversion of hourly volumes to count depends on size of bin.
These bin counts were determined by looking at the most common bin gap using:
bdit_data-sources/volumes/vds/exploration/time_gaps.sql */

SELECT 
    d.division_id,
    d.vdsconfig_uid,
    d.entity_location_uid,
    c.lanes AS num_lanes,
    d.datetime_15min,
    SUM(d.volume_veh_per_hr) / 4 / b.expected_bins AS count_15min,
    -- / 4 to convert hourly volume to 15 minute volume
    -- / (expected_bins) to get average 15 minute volume depending on bin size
    -- (assumes blanks are 0)
    b.expected_bins,
    COUNT(*) AS num_obs,
    COUNT(DISTINCT d.lane) AS num_distinct_lanes
FROM vds.raw_vdsdata AS d
JOIN vds.vdsconfig AS c ON
    d.vds_id = c.vds_id
    AND d.division_id = c.division_id
    AND d.datetime_15min >= c.start_timestamp
    AND (
        d.datetime_15min < c.end_timestamp
        OR c.end_timestamp IS NULL), --no end date
    LATERAL(
        SELECT CASE
                WHEN c.detector_id LIKE 'D%' AND c.division_id = 2
                    THEN 45 --20 sec bins
                WHEN c.detector_id LIKE ANY('{"YONGE HEATH%", "YONGE DAVISVILLE%", "BCT%"}')
                    THEN 1 --15 min bins
                WHEN c.detector_id LIKE ANY(
                        '{"%SMARTMICRO%", "%YONGE AND ROXBOROUGH%"}'
                    ) THEN 3 --5 min bins
                WHEN c.division_id = 8001
                    THEN 1 --15 min bins
            END AS expected_bins
    ) AS b
WHERE
    d.division_id = 2
    AND d.dt >= '{{ ds }} 00:00:00'::timestamp --'2023-07-05 00:00:00'::timestamp
    AND d.dt < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY' --'2023-07-06 00:00:00'::timestamp
    AND vdsconfig_uid IS NOT NULL
    AND entity_location_uid IS NOT NULL
GROUP BY
    d.division_id,
    d.vdsconfig_uid,
    d.entity_location_uid,
    c.lanes,
    b.expected_bins,
    d.datetime_15min
ON CONFLICT DO NOTHING;