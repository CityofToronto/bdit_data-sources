UPDATE prj_volume.scoot_detectors
SET direction='SB',sideofint='E'
WHERE det='30711E1' and px=204;

UPDATE prj_volume.scoot_detectors
SET direction='SB',sideofint='S'
WHERE det='30711P1' and px=205;

UPDATE prj_volume.scoot_detectors
SET direction='WB',sideofint='W'
WHERE det='30621U1' and px=205;

UPDATE prj_volume.scoot_detectors
SET direction='WB',sideofint='W'
WHERE det='30621U2' and px=205;

UPDATE prj_volume.scoot_detectors
SET direction='NB',sideofint='N'
WHERE det='30771J1' and px=205;

UPDATE prj_volume.scoot_detectors
SET sideofint = 'W'
WHERE px=1871 and sideofint='NW';

UPDATE prj_volume.scoot_detectors
SET sideofint = 'S'
WHERE px=1871 and sideofint='SW';

UPDATE prj_volume.scoot_detectors
SET sideofint = 'N'
WHERE px=1871 and det='54221J1';

