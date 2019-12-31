/*Data Validation of the top 10 imbalanced route: \
(analysis_id = 1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 1453464, 1453483, 1454196, 1454209, 1454352, 1454366, 1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400) \
NOTE: No data found for 1455243 , 1455400  for bt\
Period: 2 days (From '2019-10-09 00:00:00' to '2019-10-10 23:59:00')*/

--Create view for that of here (NO KING STREET)
CREATE MATERIALIZED VIEW jchew.here_top10_2days_no_king AS
SELECT a.analysis_id, a.street_name, a.direction, a.from_intersection, a.to_intersection,
SUM(total) AS sum_here_obs, SUM(length) AS sum_length
FROM
(SELECT analysis_id, street_name, direction, from_intersection, to_intersection, link_dir
FROM jchew.validation_bt_here_no_king bt
WHERE analysis_id IN (1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 1453464, 1453483, 1454196, 1454209, 
1454352, 1454366, 1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400)
) a
LEFT JOIN
(SELECT link_dir, count(tx) AS total, length FROM here.ta
WHERE tx BETWEEN '2019-10-09 00:00:00' AND '2019-10-10 23:59:00'
GROUP BY link_dir,length) b
USING (link_dir)
GROUP BY analysis_id, street_name, direction, from_intersection, to_intersection


--ratio for here
CREATE OR REPLACE VIEW jchew.ratio_here_top10_2days_no_king AS
WITH X AS (
SELECT a.analysis_id AS analysis_id_1, 
	b.analysis_id AS analysis_id_2, a.street_name,
	a.direction AS eb_nb, b.direction AS wb_sb, 
	a.from_intersection AS intersection_1, a.to_intersection AS intersection_2,
	a.sum_here_obs AS eb_nb_obs, b.sum_here_obs AS wb_sb_obs
	FROM jchew.here_top10_2days_no_king a
JOIN jchew.here_top10_2days_no_king b
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

--comparing both bt and here ratio
SELECT b.analysis_id_1, b.analysis_id_2, b.street_name, 
b.eb_nb, b.wb_sb, b.intersection_1, b.intersection_2, 
a.eb_nb_obs AS bt_eb_nb_obs, a.wb_sb_obs AS bt_wb_sb_obs,
b.eb_nb_obs AS here_eb_nb_obs, b.wb_sb_obs AS here_wb_sb_obs,
a."EB/WB or NB/SB Ratio" AS bt_ratio,
a."Bias Towards" AS bt_bias,
b."EB/WB or NB/SB Ratio" AS here_ratio,
b."Bias Towards" AS here_bias
FROM jchew.ratio_bt_top10_2days a
RIGHT JOIN jchew.ratio_here_top10_2days_no_king b
USING (analysis_id_1)