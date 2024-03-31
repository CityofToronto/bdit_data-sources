/* 
If you add new values to expected_bins, you will need to backfill vds.counts_15min (and _bylane). 
If you find yourself updating a previous error with expected_bins value, 
you will need to update the calculated values in vds.counts_15min (AND SIMILARLY FOR vds.counts_15min_bylane) eg: 
*/

--some of these were mistakenly set to 1 instead of 3 expected_bins. 
WITH updates AS (
    SELECT
        c15.volumeuid,
        c15.count_15min * c15.expected_bins / 3 AS count_15min_updated
    FROM vds.vdsconfig AS c
    JOIN vds.counts_15min AS c15 ON c.uid = c15.vdsconfig_uid
    WHERE (
            c.detector_id LIKE ANY('{"%YONGE HEATH%", "%YONGE DAVISVILLE%"}')
            OR c.vds_id IN (6949838, 6949843, 6949845) --new lakeshore smartmicro sensors
            OR (c.vds_id >= 7011490 AND c.vds_id <= 7011519) --new lakeshore smartmicro sensors
        )
        AND c15.expected_bins != 3
)

UPDATE vds.counts_15min AS c15
SET count_15min = updates.count_15min_updated, expected_bins = 3
FROM updates
WHERE c15.volumeuid = updates.volumeuid;

--15min_bylane:
WITH updates AS (
    SELECT
        c15.volumeuid,
        c15.count_15min * c15.expected_bins / 3 AS count_15min_updated
    FROM vds.vdsconfig AS c
    JOIN vds.counts_15min_bylane AS c15 ON c.uid = c15.vdsconfig_uid
    WHERE (
            c.detector_id LIKE ANY('{"%YONGE HEATH%", "%YONGE DAVISVILLE%"}')
            OR c.vds_id IN (6949838, 6949843, 6949845) --new lakeshore smartmicro sensors
            OR (c.vds_id >= 7011490 AND c.vds_id <= 7011519) --new lakeshore smartmicro sensors
        )
        AND c15.expected_bins != 3
)

UPDATE vds.counts_15min AS c15
SET count_15min = updates.count_15min_updated, expected_bins = 3
FROM updates
WHERE c15.volumeuid = updates.volumeuid
