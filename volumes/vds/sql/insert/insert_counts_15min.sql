--Aggregate `vds.raw_vdsdata` into table `vds.counts_15min` by detector / 15min bins. (all lanes)
INSERT INTO vds.counts_15min (division_id, vds_id, detector_id, num_lanes, datetime_15min,
    count_15min, expected_bins, num_obs, num_distinct_lanes)

/* Conversion of hourly volumes to count depends on size of bin.
These bin counts were determined by looking at the most common bin gap using:
bdit_data-sources/volumes/vds/exploration/time_gaps.sql */
WITH detector_inventory AS (
    SELECT
        *, 
        CASE
            WHEN detector_id LIKE 'D%' AND division_id = 2
                THEN 45 --20 sec bins
            WHEN detector_id LIKE ANY('{"YONGE HEATH%", "YONGE DAVISVILLE%", "BCT%"}')
                THEN 1 --15 min bins
            WHEN detector_id LIKE ANY(
                    '{"YONGE & DAVENPORT SMARTMICRO%", "%YONGE AND ROXBOROUGH%"}'
                ) THEN 3 --5 min bins
        END AS expected_bins
    FROM vds.vdsconfig 
)

SELECT 
    d.division_id,
    d.vds_id,
    c.detector_id,
    c.lanes AS num_lanes,
    d.datetime_15min,
    SUM(d.volume_veh_per_hr) / 4 / c.expected_bins AS count_15min,
        -- / 4 to convert hourly volume to 15 minute volume
        -- / (expected_bins) to get average 15 minute volume depending on bin size
        -- (assumes blanks are 0)
    c.expected_bins,
    COUNT(*) AS num_obs,
    COUNT(DISTINCT d.lane) AS num_distinct_lanes
FROM vds.raw_vdsdata AS d
JOIN detector_inventory AS c ON
    d.vds_id = c.vds_id
    AND d.division_id = c.division_id
    AND d.datetime_15min >= c.start_timestamp
    AND (
        d.datetime_15min <= c.end_timestamp
        OR c.end_timestamp IS NULL) --no end date
WHERE 
    d.datetime_15min >= '{{ ds }} 00:00:00'::timestamp --'2023-07-05 00:00:00'::timestamp
    AND d.datetime_15min < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY' --'2023-07-06 00:00:00'::timestamp
GROUP BY
    d.division_id,
    d.vds_id,
    c.detector_id,
    c.lanes,
    c.expected_bins,
    d.datetime_15min
ON CONFLICT DO NOTHING;