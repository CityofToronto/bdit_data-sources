--INSERT 0 19955760
--Query returned successfully in 3 min 18 secs.

INSERT INTO vds.counts_15min (detector_id, division_id, vds_id, num_lanes, datetime_15min, volume_15min)

SELECT DISTINCT ON (v15.detector_id, v15.datetime_bin)
    v15.detector_id,
    c.division_id,
    c.vds_id,
    c.lanes,
    v15.datetime_bin,
    v15.volume_15min
FROM rescu.volumes_15min AS v15
INNER JOIN vds.vdsconfig AS c ON --don't want to include the truncated detector_id bc we can't be sure which they correspond to. 
    v15.detector_id = c.detector_id 
    AND v15.datetime_bin >= c.start_timestamp
    AND (
        v15.datetime_bin <= c.end_timestamp
        OR c.end_timestamp IS NULL
    )
WHERE v15.datetime_bin <= '2021-10-31'
ORDER BY
    v15.detector_id ASC,
    v15.datetime_bin ASC,
    c.start_timestamp DESC,
    