--set some misc smartmicro sensors once to avoid encoding one-off logic into function
UPDATE gwolofs.detector_inventory AS di
SET det_type = CASE WHEN
    (c.detector_id::text ~~ ANY ('{"%YONGE HEATH%","%YONGE DAVISVILLE%","%YONGE AND ROXBOROUGH%"}'::text[]))
    --new lakeshore/spadina smartmicro sensors
    OR (c.vds_id = ANY (ARRAY[6949838, 6949843, 6949845, 7030552, 7030554, 7030564, 7030575, 7030577, 7030578, 7030591]))
    --new lakeshore smartmicro sensors
    OR c.vds_id >= 7011490 AND c.vds_id <= 7011519
    THEN 'Smartmicro Sensors'::text
END
FROM vds.vdsconfig AS c
WHERE
    di.vdsconfig_uid = c.uid
    AND di.det_type IS NULL;

UPDATE gwolofs.detector_inventory AS di
SET expected_bins = 3
WHERE det_type = 'Smartmicro Sensors';
