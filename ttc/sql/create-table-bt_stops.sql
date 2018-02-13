DROP TABLE IF EXISTS ttc.bt_stops;

CREATE TABLE ttc.bt_stops ( 
	bts_uid serial,
	bt_id int,
	stop_uid_from int,
	stop_uid_to int
	);

INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (19, 337, 301); -- Queen EB: Roncesvalles to Dufferin
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (20, 301, 326); -- Queen EB: Dufferin to Strachan
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (21, 326, 290); -- Queen EB: Strachan to Bathurst
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (22, 290, 357); -- Queen EB: Bathurst to Spadina
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (23, 357, 330); -- Queen EB: Spadina to University
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (24, 330, 387); -- Queen EB: University to Yonge
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (25, 387, 238); -- Queen EB: Yonge to Jarvis
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (26, 238, 260); -- Queen EB: Jarvis to Parliament
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (27, 260, 218); -- Queen EB: Parliament to Broadview
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (28, 10, 261); -- Queen WB: Broadview to Parliament
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (29, 261, 239); -- Queen WB: Parliament to Jarvis
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (30, 239, 388); -- Queen WB: Jarvis to Yonge
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (31, 388, 331); -- Queen WB: Yonge to University
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (32, 331, 358); -- Queen WB: University to Spadina
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (33, 358, 291); -- Queen WB: Spadina to Bathurst
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (34, 291, 327); -- Queen WB: Bathurst to Strachan
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (35, 327, 302); -- Queen WB: Strachan to Dufferin
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (36, 302, 338); -- Queen WB: Dufferin to Roncesvalles
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (49, 351, 42); -- King EB: Roncesvalles to Dufferin
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (50, 42, 129); -- King EB: Dufferin to Strachan
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (51, 129, 7); -- King EB: Strachan to Bathurst
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (52, 7, 125); -- King EB: Bathurst to Spadina
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (53, 125, 135); -- King EB: Spadina to University
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (54, 135, 385); -- King EB: University to Yonge
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (55, 385, 76); -- King EB: Yonge to Jarvis
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (56, 76, 211); -- King EB: Jarvis to Parliament
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (57, 211, 9); -- King EB: Parliament to Broadview
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (58, 10, 212); -- King WB: Broadview to Parliament
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (59, 212, 77); -- King WB: Parliament to Jarvis
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (60, 77, 386); -- King WB: Jarvis to Yonge
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (61, 386, 136); -- King WB: Yonge to University
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (62, 136, 356); -- King WB: University to Spadina
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (63, 356, 8); -- King WB: Spadina to Bathurst
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (64, 8, 130); -- King WB: Bathurst to Strachan
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (65, 130, 43); -- King WB: Strachan to Dufferin
INSERT INTO ttc.bt_stops(bt_id, stop_uid_from, stop_uid_to) VALUES (66, 43, 338); -- King WB: Dufferin to Roncesvalles
