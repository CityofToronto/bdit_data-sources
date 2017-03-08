UPDATE scoot.scoot_detectors
SET direction='SB',sideofint='E'
WHERE det='30711E1' and px=204;

UPDATE scoot.scoot_detectors
SET direction='SB',sideofint='S'
WHERE det='30711P1' and px=205;

UPDATE scoot.scoot_detectors
SET direction='WB',sideofint='W'
WHERE det='30621U1' and px=205;

UPDATE scoot.scoot_detectors
SET direction='WB',sideofint='W'
WHERE det='30621U2' and px=205;

UPDATE scoot.scoot_detectors
SET direction='NB',sideofint='N'
WHERE det='30771J1' and px=205;

UPDATE scoot.scoot_detectors
SET sideofint = 'W'
WHERE px=1871 and sideofint='NW';

UPDATE scoot.scoot_detectors
SET sideofint = 'S'
WHERE px=1871 and sideofint='SW';

UPDATE scoot.scoot_detectors
SET sideofint = 'N'
WHERE px=1871 and det='54221J1';

UPDATE scoot.scoot_detectors
SET direction = 'SB'
WHERE px=822 and det='70211G1';

UPDATE scoot.scoot_detectors
SET direction = 'SBLT'
WHERE px=822 and det='12881L1';

UPDATE scoot.scoot_detectors
SET direction = 'NBLT'
WHERE px=623 and det='12871S1';

DELETE FROM scoot.scoot_detectors
WHERE px=1643;

INSERT INTO scoot.scoot_detectors
VALUES (1643,12851,'12851K1','EBLT','W'),(1643,12851,'12821C1','NB','N'),
		(1643,12851,'12821C2','NB','N'),(1643,12851,'12821C3','NB','N'),
		(1643,12851,'12861A1','SB','S'),(1643,12851,'12861A2','SB','S'),
		(1643,12851,'12861A3','SB','S');
