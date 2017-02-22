TRUNCATE TABLE scoot.detector_tcl;

INSERT INTO scoot.detector_tcl
SELECT det as detector, centreline_id, direction, sideofint
FROM scoot.scoot_detectors JOIN scoot.px_tcl USING (px,sideofint);

UPDATE scoot.detector_tcl
SET centreline_id = 12335506
WHERE detector='30731Z1';

UPDATE scoot.detector_tcl
SET centreline_id = 30037425, sideofint='E'
WHERE detector='30711E1';

UPDATE scoot.detector_tcl
SET centreline_id = 30037425, sideofint='E'
WHERE detector='55131D1';
