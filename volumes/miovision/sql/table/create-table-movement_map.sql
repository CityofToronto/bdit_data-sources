DROP TABLE IF EXISTS miovision.movement_map;

CREATE TABLE miovision.movement_map (
	leg_new text,
	dir text,
	leg_old text,
	movement_uid integer);

INSERT INTO miovision.movement_map VALUES ('N','NB','N',4);
INSERT INTO miovision.movement_map VALUES ('N','NB','E',3);
INSERT INTO miovision.movement_map VALUES ('N','NB','S',1);
INSERT INTO miovision.movement_map VALUES ('N','NB','W',2);

INSERT INTO miovision.movement_map VALUES ('N','SB','N',4);
INSERT INTO miovision.movement_map VALUES ('N','SB','N',3);
INSERT INTO miovision.movement_map VALUES ('N','SB','N',1);
INSERT INTO miovision.movement_map VALUES ('N','SB','N',2);

INSERT INTO miovision.movement_map VALUES ('E','EB','E',4);
INSERT INTO miovision.movement_map VALUES ('E','EB','S',3);
INSERT INTO miovision.movement_map VALUES ('E','EB','W',1);
INSERT INTO miovision.movement_map VALUES ('E','EB','N',2);

INSERT INTO miovision.movement_map VALUES ('E','WB','E',4);
INSERT INTO miovision.movement_map VALUES ('E','WB','E',3);
INSERT INTO miovision.movement_map VALUES ('E','WB','E',1);
INSERT INTO miovision.movement_map VALUES ('E','WB','E',2);

INSERT INTO miovision.movement_map VALUES ('S','SB','S',4);
INSERT INTO miovision.movement_map VALUES ('S','SB','W',3);
INSERT INTO miovision.movement_map VALUES ('S','SB','N',1);
INSERT INTO miovision.movement_map VALUES ('S','SB','E',2);

INSERT INTO miovision.movement_map VALUES ('S','NB','S',4);
INSERT INTO miovision.movement_map VALUES ('S','NB','S',3);
INSERT INTO miovision.movement_map VALUES ('S','NB','S',1);
INSERT INTO miovision.movement_map VALUES ('S','NB','S',2);

INSERT INTO miovision.movement_map VALUES ('W','WB','W',4);
INSERT INTO miovision.movement_map VALUES ('W','WB','N',3);
INSERT INTO miovision.movement_map VALUES ('W','WB','E',1);
INSERT INTO miovision.movement_map VALUES ('W','WB','S',2);

INSERT INTO miovision.movement_map VALUES ('W','EB','W',4);
INSERT INTO miovision.movement_map VALUES ('W','EB','W',3);
INSERT INTO miovision.movement_map VALUES ('W','EB','W',1);
INSERT INTO miovision.movement_map VALUES ('W','EB','W',2);

INSERT INTO miovision.movement_map VALUES ('N','EB','N',5);
INSERT INTO miovision.movement_map VALUES ('N','WB','N',6);
INSERT INTO miovision.movement_map VALUES ('E','SB','E',5);
INSERT INTO miovision.movement_map VALUES ('E','NB','E',6);
INSERT INTO miovision.movement_map VALUES ('S','WB','S',5);
INSERT INTO miovision.movement_map VALUES ('S','EB','S',6);
INSERT INTO miovision.movement_map VALUES ('W','NB','W',5);
INSERT INTO miovision.movement_map VALUES ('W','SB','W',6);