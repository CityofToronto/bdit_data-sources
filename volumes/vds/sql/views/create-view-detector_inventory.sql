--DROP VIEW vds.detector_inventory;
CREATE VIEW vds.detector_inventory AS (
    SELECT
        c.uid,
        c.detector_id,
        c.division_id,
        dtypes.det_type,
        CASE dtypes.det_type = 'RESCU Detectors'
            WHEN TRUE THEN CASE substring(c.detector_id, 2, 1)
                WHEN 'N' THEN 'DVP/Allen North' --North of Don Mills 
                WHEN 'S' THEN 'DVP South' --South of Don Mills 
                WHEN 'E' THEN 'Gardiner/Lakeshore East' --East of Yonge
                WHEN 'W' THEN 'Gardiner/Lakeshore West' --West of Yonge
                WHEN 'K' THEN 'Kingston Rd'
                END
        END AS det_loc,
        CASE dtypes.det_type = 'RESCU Detectors'
            WHEN TRUE THEN CASE substring(c.detector_id, 9, 1)
                WHEN 'D' THEN 'DVP'
                WHEN 'L' THEN 'Lakeshore'
                WHEN 'G' THEN 'Gardiner'
                WHEN 'A' THEN 'Allen'
                WHEN 'K' THEN 'Kingston Rd'
                WHEN 'R' THEN 'On-Ramp'
                END
        END AS det_group,
        CASE dtypes.det_type = 'RESCU Detectors'
            WHEN TRUE THEN CASE substring(c.detector_id, 8, 1)
                WHEN 'E' THEN 'Eastbound'
                WHEN 'W' THEN 'Westbound'
                WHEN 'S' THEN 'Southbound'
                WHEN 'N' THEN 'Northbound'
                END 
        END AS direction,
        --new cases need to be updated manually and then updated in vds.count_15min%.
        CASE
            WHEN c.division_id = 8001 THEN 1 --15 min bins
            --remainder are division_id = 2
            WHEN dtypes.det_type = 'RESCU Detectors' THEN 45 --20 sec bins
            WHEN dtypes.det_type = 'Blue City AI' THEN 1 --15 min bins
            WHEN dtypes.det_type = 'Smartmicro Sensors' THEN 3 --5 min bins
        END AS expected_bins
    FROM vds.vdsconfig AS c,
        LATERAL(
            SELECT CASE
                WHEN c.detector_id LIKE 'D%' AND c.division_id = 2 THEN 'RESCU Detectors'
                WHEN c.detector_id SIMILAR TO 'PX[0-9]{4}-DET%' AND c.division_id = 8001 THEN 'Signal Detectors'
                WHEN c.detector_id SIMILAR TO 'PX[0-9]{4}-SF%' AND c.division_id = 8001 THEN 'Signal Special Function'
                WHEN c.detector_id SIMILAR TO 'PX[0-9]{4}-PE%' AND c.division_id = 8001 THEN 'Signal Preemption'
                WHEN c.detector_id LIKE 'BCT%' THEN 'Blue City AI'
                WHEN c.detector_id LIKE ANY('{"%SMARTMICRO%", "%YONGE HEATH%", "%YONGE DAVISVILLE%", "%YONGE AND ROXBOROUGH%"}')
                    OR c.vds_id IN (6949838, 6949843, 6949845) --new lakeshore smartmicro sensors
                    OR (c.vds_id >= 7011490 AND c.vds_id <= 7011519) --new lakeshore smartmicro sensors
                    THEN 'Smartmicro Sensors'
                END AS det_type
        ) AS dtypes
);

COMMENT ON VIEW vds.detector_inventory IS 'Centralize information about
expected bin width for each detector to be used in converting vehicle per hour to 
vehicle count. May need to periodically update `expected_bins` column using
bdit_data-sources/volumes/vds/exploration/time_gaps.sql';

ALTER VIEW vds.detector_inventory OWNER TO vds_admins;
GRANT SELECT ON vds.detector_inventory TO bdit_humans;
GRANT SELECT ON vds.detector_inventory TO vds_bot;