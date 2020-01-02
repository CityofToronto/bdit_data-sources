-- speed = length / median (tt)

/*Data Validation of the top 10 imbalanced route: \
(analysis_id = 1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 1453464, 1453483, 1454196, 1454209, 1454352, 1454366, 1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400) \
NOTE: No data found for 1455243 , 1455400  for bt\
Period: 2 days (From '2019-10-09 00:00:00' to '2019-10-10 23:59:00')*/


--Create view for that of bt
CREATE OR REPLACE VIEW jchew.bt_top10_2days_spd AS
WITH X AS 
(SELECT bt.analysis_id, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY tt) AS median_tt, l.length
FROM bluetooth.aggr_5min bt 
RIGHT JOIN bluetooth.segments l
USING (analysis_id)
WHERE bt.datetime_bin BETWEEN '2019-10-09 00:00:00' AND '2019-10-10 23:59:00'
GROUP BY bt.analysis_id, l.length)
SELECT X.analysis_id, seg.bt_id, 
seg.street_name, seg.direction, seg.from_intersection, seg.to_intersection, 
X.length, X.median_tt, (X.length*0.001)/(X.median_tt/3600) AS speed
FROM X
JOIN king_pilot.bt_segments seg
USING (analysis_id)
WHERE seg.analysis_id IN (1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 1453464, 1453483, 1454196, 1454209,
						  1454352, 1454366, 1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400)

--ratio for bt
CREATE OR REPLACE VIEW jchew.ratio_bt_top10_2days_spd AS
WITH X AS (
SELECT a.analysis_id AS analysis_id_1, a.bt_id AS bt_id_1, 
	b.analysis_id AS analysis_id_2, b.bt_id AS bt_id_2, a.street_name, 
	a.direction AS eb_nb, b.direction AS wb_sb, 
	a.from_intersection AS intersection_1, a.to_intersection AS intersection_2,
	a.speed AS eb_nb_spd, b.speed AS wb_sb_spd
	FROM jchew.bt_top10_2days_spd a
JOIN jchew.bt_top10_2days_spd b
ON a.street_name = b .street_name AND a.from_intersection = b.to_intersection 
AND a.to_intersection=b.from_intersection 
WHERE a.direction IN ('EB', 'NB')
)
SELECT *,
CASE WHEN X.eb_nb_spd > X.wb_sb_spd THEN (X.eb_nb_spd - X.wb_sb_spd)
WHEN X.eb_nb_spd < X.wb_sb_spd THEN (X.eb_nb_spd - X.wb_sb_spd)
END AS "Speed Difference",
 CASE
WHEN X.eb_nb = 'EB' THEN CASE
	WHEN X.eb_nb_spd < X.wb_sb_spd THEN 'WB'
    WHEN X.eb_nb_spd > X.wb_sb_spd THEN 'EB'
    END
WHEN X.eb_nb = 'NB' THEN CASE
    WHEN X.eb_nb_spd < X.wb_sb_spd THEN 'SB'
    WHEN X.eb_nb_spd > X.wb_sb_spd THEN 'NB'
    END
        END AS "Bias Towards"
FROM X
ORDER BY analysis_id_1