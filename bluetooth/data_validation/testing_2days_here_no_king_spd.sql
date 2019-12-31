--Create view for that of here spd (NO KING STREET)
CREATE OR REPLACE VIEW jchew.here_top10_2days_no_king_spd AS 
SELECT a.analysis_id, a.street_name, a.direction, a.from_intersection, a.to_intersection,
avg(pct_50) AS speed
FROM
(SELECT analysis_id, street_name, direction, from_intersection, to_intersection, link_dir
FROM jchew.validation_bt_here_no_king bt
WHERE analysis_id IN (1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 1453464, 1453483, 1454196, 1454209, 
1454352, 1454366, 1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400)
) a
LEFT JOIN
(SELECT link_dir, pct_50 FROM here.ta
WHERE tx BETWEEN '2019-10-09 00:00:00' AND '2019-10-10 23:59:00'
) b
USING (link_dir)
GROUP BY analysis_id, street_name, direction, from_intersection, to_intersection


--ratio for here spd
CREATE OR REPLACE VIEW jchew.ratio_here_top10_2days_no_king_spd AS
WITH X AS (
SELECT a.analysis_id AS analysis_id_1,
	b.analysis_id AS analysis_id_2, a.street_name, 
	a.direction AS eb_nb, b.direction AS wb_sb, 
	a.from_intersection AS intersection_1, a.to_intersection AS intersection_2,
	a.speed AS eb_nb_spd, b.speed AS wb_sb_spd
	FROM jchew.here_top10_2days_no_king_spd a
JOIN jchew.here_top10_2days_no_king_spd b
ON a.street_name = b .street_name AND a.from_intersection = b.to_intersection 
AND a.to_intersection=b.from_intersection 
WHERE a.direction IN ('EB', 'NB')
)
SELECT *,
CASE WHEN X.eb_nb_spd > X.wb_sb_spd THEN (X.eb_nb_spd * 1.0) / (X.eb_nb_spd + X.wb_sb_spd)
WHEN X.eb_nb_spd < X.wb_sb_spd THEN (X.wb_sb_spd * 1.0) / (X.eb_nb_spd + X.wb_sb_spd)
END AS "EB/WB or NB/SB Ratio",
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

--comparing both bt and here ratio for spd
SELECT b.analysis_id_1, b.analysis_id_2, b.street_name, 
b.eb_nb, b.wb_sb, b.intersection_1, b.intersection_2, 
a.eb_nb_spd AS bt_eb_nb_spd, a.wb_sb_spd AS bt_wb_sb_spd,
b.eb_nb_spd AS here_eb_nb_spd, b.wb_sb_spd AS here_wb_sb_spd,
a."EB/WB or NB/SB Ratio" AS bt_ratio,
a."Bias Towards" AS bt_bias,
b."EB/WB or NB/SB Ratio" AS here_ratio,
b."Bias Towards" AS here_bias
FROM jchew.ratio_bt_top10_2days_spd a
RIGHT JOIN jchew.ratio_here_top10_2days_no_king_spd b
USING (analysis_id_1)