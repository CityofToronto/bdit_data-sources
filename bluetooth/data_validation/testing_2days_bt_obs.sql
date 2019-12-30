/*Data Validation of the top 10 imbalanced route: \
(analysis_id = 1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 1453464, 1453483, 1454196, 1454209, 1454352, 1454366, 1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400) \
NOTE: No data found for 1455243 , 1455400  for bt\
Period: 2 days (From '2019-10-09 00:00:00' to '2019-10-10 23:59:00')*/

--Create view for that of bt
CREATE OR REPLACE VIEW jchew.bt_top10_2days AS
SELECT bt.analysis_id, seg.bt_id, 
seg.street_name, seg.direction, seg.from_intersection, seg.to_intersection,
sum(obs) AS sum_bt_obs
FROM bluetooth.aggr_5min bt
JOIN king_pilot.bt_segments seg
USING (analysis_id)
WHERE bt.datetime_bin BETWEEN '2019-10-09 00:00:00' AND '2019-10-10 23:59:00'
AND seg.analysis_id IN (1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 1453464, 1453483, 1454196, 1454209,
					1454352, 1454366, 1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400)
GROUP BY bt.analysis_id, seg.bt_id, seg.street_name, seg.direction, seg.from_intersection, seg.to_intersection

--ratio for bt
CREATE OR REPLACE VIEW jchew.ratio_bt_top10_2days AS
WITH X AS (
SELECT a.analysis_id AS analysis_id_1, a.bt_id AS bt_id_1, 
	b.analysis_id AS analysis_id_2, b.bt_id AS bt_id_2, a.street_name, 
	a.direction AS eb_nb, b.direction AS wb_sb, 
	a.from_intersection AS intersection_1, a.to_intersection AS intersection_2,
	a.sum_bt_obs AS eb_nb_obs, b.sum_bt_obs AS wb_sb_obs
	FROM jchew.bt_top10_2days a
JOIN jchew.bt_top10_2days b
ON a.street_name = b .street_name AND a.from_intersection = b.to_intersection 
AND a.to_intersection=b.from_intersection 
WHERE a.direction IN ('EB', 'NB')
)
SELECT *,
CASE WHEN X.eb_nb_obs > X.wb_sb_obs THEN (X.eb_nb_obs * 1.0) / (X.eb_nb_obs + X.wb_sb_obs)
WHEN X.eb_nb_obs < X.wb_sb_obs THEN (X.wb_sb_obs * 1.0) / (X.eb_nb_obs + X.wb_sb_obs)
END AS "EB/WB or NB/SB Ratio",
 CASE
WHEN X.eb_nb = 'EB' THEN CASE
	WHEN X.eb_nb_obs < X.wb_sb_obs THEN 'WB'
    WHEN X.eb_nb_obs > X.wb_sb_obs THEN 'EB'
    END
WHEN X.eb_nb = 'NB' THEN CASE
    WHEN X.eb_nb_obs < X.wb_sb_obs THEN 'SB'
    WHEN X.eb_nb_obs > X.wb_sb_obs THEN 'NB'
    END
        END AS "Bias Towards"
FROM X
ORDER BY analysis_id_1