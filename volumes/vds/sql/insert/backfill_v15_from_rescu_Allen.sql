--special backfill case for allen detectors which have different format in rescu vs vds:
--"detector_id" (vds): "DS0020DSA ALLEN03", "detector_id" (rescu): "DS0020DSA"
INSERT INTO vds.counts_15min (division_id, vdsconfig_uid, entity_location_uid, num_lanes, datetime_15min,
    count_15min, expected_bins)

--prepare detector_id_trunc to match rescu format. 
WITH vdsconfig_allen AS (
    --checked to make sure truncating detector_id didn't create any overlapping ranges for detector_id_trunc + dates. 
    SELECT
        uid,
        vds_id,
        division_id,
        substring(detector_id, 1, 9) AS detector_id_trunc,
        start_timestamp,
        end_timestamp,
        lanes
    FROM vds.vdsconfig
    WHERE
        division_id = 2
        AND substring(detector_id, 1, 1) = 'D'
        AND substring(detector_id, 9, 1) = 'A'
        --these detector_ids don't require special case: 
        AND substring(detector_id, 1, 9) != detector_id

)

--get old rescu schema data in format for vds.counts_15min for allen sensors
SELECT
    c.division_id,
    c.uid AS vdsconfig_uid,
    e.uid AS entity_location_uid,
    c.lanes AS num_lanes,
    v15.datetime_bin,
    v15.volume_15min AS count_15min,
    di.expected_bins
FROM rescu.volumes_15min AS v15
INNER JOIN vdsconfig_allen AS c ON
    --join on detector_id_trunc instead of detector_id
    upper(v15.detector_id) = c.detector_id_trunc 
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
LEFT JOIN vds.detector_inventory AS di
    ON di.vdsconfig_uid = c.uid
    AND di.entity_location_uid = e.uid
WHERE v15.datetime_bin < '2021-11-01 00:00:00'::timestamp
ORDER BY
    v15.detector_id ASC,
    v15.datetime_bin ASC,
    c.start_timestamp DESC
