--Create view for bt spd
CREATE OR REPLACE VIEW jchew.bt_top10_1month_spd AS
WITH X AS 
(SELECT bt.analysis_id, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY tt) AS median_tt, l.length
FROM bluetooth.aggr_5min bt 
RIGHT JOIN bluetooth.segments l
USING (analysis_id)
WHERE bt.datetime_bin BETWEEN '2019-11-01 00:00:00' AND '2019-11-30 23:59:00'
GROUP BY bt.analysis_id, l.length)
SELECT X.analysis_id, seg.bt_id, 
seg.street_name, seg.direction, seg.from_intersection, seg.to_intersection, 
X.length, X.median_tt, (X.length*0.001)/(X.median_tt/3600) AS speed
FROM X
JOIN king_pilot.bt_segments seg
USING (analysis_id)
WHERE seg.analysis_id IN (1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 1453464, 1453483, 1454196, 1454209,
                          1454352, 1454366, 1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400)

--ratio for bt spd
CREATE OR REPLACE VIEW jchew.ratio_bt_top10_1month_spd AS
WITH X AS (
SELECT a.analysis_id AS analysis_id_1, a.bt_id AS bt_id_1, 
    b.analysis_id AS analysis_id_2, b.bt_id AS bt_id_2, a.street_name, 
    a.direction AS eb_nb, b.direction AS wb_sb, 
    a.from_intersection AS intersection_1, a.to_intersection AS intersection_2,
    a.speed AS eb_nb_spd, b.speed AS wb_sb_spd
    FROM jchew.bt_top10_1month_spd a
JOIN jchew.bt_top10_1month_spd b
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

--Create view for here spd
CREATE OR REPLACE VIEW jchew.here_top10_1month_spd AS 
SELECT a.analysis_id, a.street_name, a.direction, a.from_intersection_name, a.to_intersection_name,
avg(pct_50) AS speed
FROM
(SELECT analysis_id, street_name, direction, from_intersection_name, to_intersection_name, pp_link_dir
FROM jchew.validation_bt_here bt
WHERE analysis_id IN (1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 1453464, 1453483, 1454196, 1454209, 
1454352, 1454366, 1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400)
) a
LEFT JOIN
(SELECT link_dir, pct_50 FROM here.ta
WHERE tx BETWEEN '2019-11-01 00:00:00' AND '2019-11-30 23:59:00'
) b
ON a.pp_link_dir = b.link_dir
GROUP BY analysis_id, street_name, direction, from_intersection_name, to_intersection_name

--ratio for here spd
CREATE OR REPLACE VIEW jchew.ratio_here_top10_1month_spd AS
WITH X AS (
SELECT a.analysis_id AS analysis_id_1,
    b.analysis_id AS analysis_id_2, a.street_name, 
    a.direction AS eb_nb, b.direction AS wb_sb, 
    a.from_intersection_name AS intersection_1, a.to_intersection_name AS intersection_2,
    a.speed AS eb_nb_spd, b.speed AS wb_sb_spd
    FROM jchew.here_top10_1month_spd a
JOIN jchew.here_top10_1month_spd b
ON a.street_name = b .street_name AND a.from_intersection_name = b.to_intersection_name 
AND a.to_intersection_name = b.from_intersection_name 
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

--to compare
SELECT b.analysis_id_1, b.analysis_id_2, b.street_name, 
b.eb_nb, b.wb_sb, b.intersection_1, b.intersection_2, 
a.eb_nb_spd AS bt_eb_nb_spd, a.wb_sb_spd AS bt_wb_sb_spd,
b.eb_nb_spd AS here_eb_nb_spd, b.wb_sb_spd AS here_wb_sb_spd,
a."Speed Difference" AS bt_diff_spd,
b."Speed Difference" AS here_diff_spd,
a."Bias Towards" AS bt_bias,
b."Bias Towards" AS here_bias
FROM jchew.ratio_bt_top10_1month_spd a
RIGHT JOIN jchew.ratio_here_top10_1month_spd b
USING (analysis_id_1)