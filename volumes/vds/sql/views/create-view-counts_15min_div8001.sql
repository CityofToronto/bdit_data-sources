--add entity_location_uid fkey in detector inventory branch

CREATE VIEW vds.counts_15min_div8001 AS (
    SELECT
        c.uid AS vdsconfig_uid, 
        c.detector_id,
        d.division_id,
        d.vds_id,
        d.datetime_15min,
        c.lanes AS num_lanes,
        SUM(d.volume_veh_per_hr) / 4 / e.expected_bins AS count_15min,
        e.expected_bins, 
        COUNT(*) AS num_obs,
        COUNT(DISTINCT(lane)) AS num_distinct_lanes
    FROM vds.raw_vdsdata AS d
    JOIN vds.vdsconfig AS c ON 
        d.vds_id = c.vds_id
        AND d.division_id = c.division_id
        AND d.dt >= c.start_timestamp
        AND (
            d.dt < c.end_timestamp
            OR c.end_timestamp IS NULL) --no end date
    JOIN vds.detectors_expected_bins AS e ON c.uid = e.uid 
    WHERE d.division_id = 8001
    GROUP BY
        c.uid,
        c.detector_id,
        d.division_id,
        d.vds_id,
        d.datetime_15min,
        c.lanes,
        e.expected_bins
)