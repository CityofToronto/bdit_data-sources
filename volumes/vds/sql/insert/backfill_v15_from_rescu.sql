--INSERT 0 19549192
--Query returned successfully in 10 min 12 secs.

INSERT INTO vds.counts_15min (division_id, vdsconfig_uid, entity_location_uid, num_lanes, datetime_15min,
    count_15min, expected_bins)
    
SELECT
    c.division_id,
    c.uid AS vdsconfig_uid,
    e.uid AS entity_location_uid,
    c.lanes AS num_lanes,
    v15.datetime_bin,
    v15.volume_15min AS count_15min,
    eb.expected_bins
FROM rescu.volumes_15min AS v15
INNER JOIN vds.vdsconfig AS c ON --don't want to include the truncated detector_id bc we can't be sure which they correspond to. 
    upper(v15.detector_id) = c.detector_id 
    AND c.division_id = 2
    AND v15.datetime_bin >= c.start_timestamp
    AND (
        v15.datetime_bin < c.end_timestamp
        OR c.end_timestamp IS NULL
    )
INNER JOIN vds.entity_locations AS e ON
    e.entity_id = c.vds_id 
    AND e.division_id = 2
    AND v15.datetime_bin >= e.start_timestamp
    AND (
        v15.datetime_bin < e.end_timestamp
        OR e.end_timestamp IS NULL
    )
LEFT JOIN vds.detectors_expected_bins AS eb ON eb.uid = c.uid
WHERE v15.datetime_bin < '2021-11-01'
ORDER BY
    v15.detector_id ASC,
    v15.datetime_bin ASC,
    c.start_timestamp DESC
