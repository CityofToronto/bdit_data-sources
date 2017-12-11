DROP TABLE IF EXISTS king_pilot.bt_ttc_tt;

CREATE TABLE king_pilot.bt_ttc_tt (
	btt_uid serial,
	bt_id int,
	start_dt timestamp without time zone,
	end_dt timestamp without time zone,
	tt_sec int);

INSERT INTO king_pilot.bt_ttc_tt(bt_id, start_dt, end_dt, tt_sec)
SELECT bt_id, B.dep_time AS start_dt, C.dep_time AS end_dt, EXTRACT(EPOCH FROM (C.dep_time - B.dep_time)) AS tt_sec
FROM ttc.bt_stops A
INNER JOIN ttc.trip_stops B ON B.stop_uid = A.stop_uid_from
INNER JOIN ttc.trip_stops C ON C.stop_uid = A.stop_uid_to AND B.trip_uid = C.trip_uid
ORDER BY B.dep_time;