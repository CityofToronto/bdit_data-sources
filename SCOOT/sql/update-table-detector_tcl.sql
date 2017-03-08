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

UPDATE scoot.detector_tcl
SET centreline_id = 20054531, sideofint='E'
WHERE detector='30421X1';

UPDATE scoot.detector_tcl
SET centreline_id = 20054531, sideofint='E'
WHERE detector='30431T1';

UPDATE scoot.detector_tcl
SET centreline_id = 20054539, sideofint='W'
WHERE detector='30411H1';

UPDATE scoot.detector_tcl
SET centreline_id = 30020765, sideofint='W'
WHERE detector='30321H1' or detector='30321H2';