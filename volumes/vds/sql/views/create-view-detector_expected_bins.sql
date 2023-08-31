DROP VIEW vds.detectors_expected_bins;
CREATE VIEW vds.detectors_expected_bins AS (
    SELECT
        uid,
        detector_id,
        CASE
            WHEN division_id = 8001
                THEN 1 --15 min bins
            --remainder are division_id = 2
            WHEN detector_id LIKE 'D%'
                THEN 45 --20 sec bins
            WHEN detector_id LIKE ANY('{"YONGE HEATH%", "YONGE DAVISVILLE%", "BCT%"}')
                THEN 1 --15 min bins
            WHEN detector_id LIKE ANY('{"%SMARTMICRO%", "%YONGE AND ROXBOROUGH%"}')
                OR vds_id IN (6949838, 6949843, 6949845) --lakeshore smartmicro sensors
                THEN 3 --5 min bins
            ELSE NULL --new cases need to be updated manually and then updated in vds.count_15min%. -- noqa: L035
        END AS expected_bins
    FROM vds.vdsconfig    
);

COMMENT ON VIEW vds.detectors_expected_bins IS 'Centralize information about
expected bin width for each detector to be used in converting vehicle per hour to 
vehicle count. May need to be periodically updated using
bdit_data-sources/volumes/vds/exploration/time_gaps.sql';

ALTER VIEW vds.detectors_expected_bins OWNER TO vds_admins;
GRANT SELECT ON vds.detectors_expected_bins TO bdit_humans;
GRANT SELECT ON vds.detectors_expected_bins TO vds_bot;