--DROP VIEW vds.detector_inventory;
CREATE OR REPLACE VIEW vds.detector_inventory AS (
    SELECT DISTINCT ON (c.uid, c.division_id)
        c.uid,
        c.detector_id,
        c.division_id,
        dtypes.det_type,
        CASE dtypes.det_type = 'RESCU Detectors'
            WHEN TRUE THEN CASE substring(substring(c.detector_id, 'D\w{8}'), 2, 1)
                WHEN 'N' THEN 'DVP/Allen North' --North of Don Mills 
                WHEN 'S' THEN 'DVP South' --South of Don Mills 
                WHEN 'E' THEN 'Gardiner/Lakeshore East' --East of Yonge
                WHEN 'W' THEN 'Gardiner/Lakeshore West' --West of Yonge
                WHEN 'K' THEN 'Kingston Rd'
            END
        END AS det_loc,
        CASE dtypes.det_type = 'RESCU Detectors'
            WHEN TRUE THEN CASE substring(substring(c.detector_id, 'D\w{8}'), 9, 1)
                WHEN 'D' THEN 'DVP'
                WHEN 'L' THEN 'Lakeshore'
                WHEN 'G' THEN 'Gardiner'
                WHEN 'A' THEN 'Allen'
                WHEN 'K' THEN 'Kingston Rd'
                WHEN 'R' THEN 'On-Ramp'
            END
        END AS det_group,
        CASE dtypes.det_type = 'RESCU Detectors'
            WHEN TRUE THEN CASE substring(substring(c.detector_id, 'D\w{8}'), 8, 1)
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
            WHEN
                dtypes.det_type = 'Smartmicro Sensors'
                OR c.detector_id SIMILAR TO 'SMARTMICRO - D\w{8}' THEN 3 --5 min bins
            WHEN dtypes.det_type = 'RESCU Detectors' THEN 45 --20 sec bins
            WHEN dtypes.det_type = 'Blue City AI' THEN 1 --15 min bins
        END AS expected_bins,
        comms.source_id AS comms_desc,
        --rescu techology type, determined by communication device in some cases.
        CASE dtypes.det_type = 'RESCU Detectors'
            WHEN TRUE THEN CASE
                WHEN lower(comms.source_id) SIMILAR TO '%smartmicro%'
                    OR lower(c.detector_id) SIMILAR TO '%smartmicro%'
                    THEN 'Smartmicro'
                WHEN lower(comms.source_id) SIMILAR TO '%whd%'
                    THEN 'Wavetronix'
                ELSE 'Inductive'
            END
        END AS det_tech
    FROM vds.vdsconfig AS c
    LEFT JOIN vds.config_comms_device AS comms
        ON comms.fss_id = c.fss_id
        AND comms.division_id = c.division_id
        AND tsrange(c.start_timestamp, COALESCE(c.end_timestamp, now()::timestamp))
            && tsrange(comms.start_timestamp, COALESCE(comms.end_timestamp, now()::timestamp)),
        LATERAL (
            SELECT CASE
                WHEN c.division_id = 2 AND (
                    c.detector_id SIMILAR TO 'D\w{8}%'
                    --smartmicro installed in place of old rescu sensors on highways
                    OR c.detector_id SIMILAR TO 'SMARTMICRO - D\w{8}'
                ) THEN 'RESCU Detectors'
                WHEN c.detector_id SIMILAR TO 'PX[0-9]{4}-DET%' AND c.division_id = 8001
                    THEN 'Signal Detectors'
                WHEN c.detector_id SIMILAR TO 'PX[0-9]{4}-SF%' AND c.division_id = 8001
                    THEN 'Signal Special Function'
                WHEN c.detector_id SIMILAR TO 'PX[0-9]{4}-PE%' AND c.division_id = 8001
                    THEN 'Signal Preemption'
                WHEN c.detector_id LIKE 'BCT%' THEN 'Blue City AI'
                WHEN
                    c.detector_id LIKE ANY(
                        '{"%SMARTMICRO%", "%YONGE HEATH%", "%YONGE DAVISVILLE%", "%YONGE AND ROXBOROUGH%"}'
                    )
                    --new lakeshore/spadina smartmicro sensors
                    OR c.vds_id IN (
                        6949838, 6949843, 6949845, 7030552, 7030554, 7030564, 7030575, 7030577
                    )
                    --new lakeshore smartmicro sensors
                    OR (
                        c.vds_id >= 7011490 AND c.vds_id <= 7011519
                    )
                        THEN 'Smartmicro Sensors'
            END AS det_type
        ) AS dtypes
    ORDER BY
        c.uid,
        c.division_id,
        comms.start_timestamp DESC --most recently installed comms
);

COMMENT ON VIEW vds.detector_inventory IS 'Centralize information about
expected bin width for each detector to be used in converting vehicle per hour to 
vehicle count. May need to periodically update `expected_bins` column using
bdit_data-sources/volumes/vds/exploration/time_gaps.sql';

ALTER VIEW vds.detector_inventory OWNER TO vds_admins;
GRANT SELECT ON vds.detector_inventory TO bdit_humans;
GRANT SELECT ON vds.detector_inventory TO vds_bot;