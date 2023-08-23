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
            ELSE NULL
        END AS expected_bins
    FROM vds.vdsconfig    
);