DROP VIEW vds.detector_inventory;
CREATE VIEW vds.detector_inventory AS (
    SELECT
        uid,
        detector_id,
        division_id,
        det_type,
        CASE det_type = 'RESCU Detectors'
            WHEN True THEN CASE substring(detector_id, 2, 1)
                WHEN 'N' THEN 'DVP/Allen North' --North of Don Mills 
                WHEN 'S' THEN 'DVP South' --South of Don Mills 
                WHEN 'E' THEN 'Gardiner/Lakeshore East' --East of Yonge
                WHEN 'W' THEN 'Gardiner/Lakeshore West' --West of Yonge
                WHEN 'K' THEN 'Kingston Rd'
            END
        END AS det_loc,
        CASE det_type = 'RESCU Detectors'
            WHEN True THEN CASE substring(detector_id, 9, 1)
                WHEN 'D' THEN 'DVP'
                WHEN 'L' THEN 'Lakeshore'
                WHEN 'G' THEN 'Gardiner'
                WHEN 'A' THEN 'Allen'
                WHEN 'K' THEN 'Kingston Rd'
                WHEN 'R' THEN 'On-Ramp'
            END
        END AS det_group,
        CASE det_type = 'RESCU Detectors'
            WHEN True THEN CASE substring(detector_id, 8, 1)
                WHEN 'E' THEN 'Eastbound'
                WHEN 'W' THEN 'Westbound'
                WHEN 'S' THEN 'Southbound'
                WHEN 'N' THEN 'Northbound'
            END 
        END AS direction,
        CASE
            WHEN division_id = 8001 THEN 1 --15 min bins
            --remainder are division_id = 2
            WHEN det_type = 'RESCU Detectors' THEN 45 --20 sec bins
            WHEN det_type = 'Blue City AI' THEN 1 --15 min bins
            WHEN det_type = 'Smartmicro Sensors' THEN 3 --5 min bins
            ELSE NULL --new cases need to be updated manually and then updated in vds.count_15min%. -- noqa: L035
        END AS expected_bins
    FROM vds.vdsconfig,
        LATERAL (
            SELECT 
                CASE
                    WHEN detector_id LIKE 'D%' AND division_id = 2 THEN 'RESCU Detectors'
                    WHEN detector_id SIMILAR TO 'PX[0-9]{4}-DET%' AND division_id = 8001 THEN 'Signal Detectors'
                    WHEN detector_id SIMILAR TO 'PX[0-9]{4}-SF%' AND division_id = 8001 THEN 'Signal Special Function'
                    WHEN detector_id SIMILAR TO 'PX[0-9]{4}-PE%' AND division_id = 8001 THEN 'Signal Preemption'
                    WHEN detector_id LIKE 'BCT%' THEN 'Blue City AI'
                    WHEN detector_id LIKE ANY ('{"%SMARTMICRO%", "%YONGE HEATH%", "%YONGE DAVISVILLE%", "%YONGE AND ROXBOROUGH%"}')
                        OR vds_id IN (6949838, 6949843, 6949845) --new lakeshore smartmicro sensors
                        OR (vds_id >= 7011490 AND vds_id <= 7011519) --new lakeshore smartmicro sensors
                        THEN 'Smartmicro Sensors'
                END AS det_type
        ) AS types
);

COMMENT ON VIEW vds.detector_inventory IS 'Centralize information about
expected bin width for each detector to be used in converting vehicle per hour to 
vehicle count. May need to periodically update `expected_bins` column using
bdit_data-sources/volumes/vds/exploration/time_gaps.sql';

ALTER VIEW vds.detector_inventory OWNER TO vds_admins;
GRANT SELECT ON vds.detector_inventory TO bdit_humans;
GRANT SELECT ON vds.detector_inventory TO vds_bot;

/* 
If you add new values to expected_bins, you will need to backfill vds.counts_15min (and _bylane). 
If you find yourself updating a previous error with expected_bins value, 
you will need to update the calculated values in vds.counts_15min (AND SIMILARLY FOR vds.counts_15min_bylane) eg: 

```
--some of these were mistakenly set to 1 instead of 3 expected_bins. 
WITH updates AS (
    SELECT volumeuid, count_15min * expected_bins / 3 AS count_15min_updated
    FROM vds.vdsconfig AS c
    JOIN vds.counts_15min AS c15 ON c.uid = c15.vdsconfig_uid
    WHERE
        detector_id LIKE ANY ('{"%YONGE HEATH%", "%YONGE DAVISVILLE%"}')
                            OR vds_id IN (6949838, 6949843, 6949845) --new lakeshore smartmicro sensors
                            OR (vds_id >= 7011490 AND vds_id <= 7011519) --new lakeshore smartmicro sensors
        AND expected_bins <> 3
)
UPDATE vds.counts_15min AS c15
SET count_15min = updates.count_15min_updated, expected_bins = 3
FROM updates
WHERE c15.volumeuid = updates.volumeuid
```
*/
