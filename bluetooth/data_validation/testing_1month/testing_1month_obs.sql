--Create view for bt obs
CREATE OR REPLACE VIEW jchew.bt_top10_1month AS
SELECT bt.analysis_id, seg.bt_id, 
seg.street_name, seg.direction, seg.from_intersection, seg.to_intersection,
sum(obs) AS sum_bt_obs
FROM bluetooth.aggr_5min bt
JOIN king_pilot.bt_segments seg
USING (analysis_id)
WHERE bt.datetime_bin BETWEEN '2019-11-01 00:00:00' AND '2019-11-30 23:59:00'
AND seg.analysis_id IN (1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 1453464, 1453483, 1454196, 1454209,
					1454352, 1454366, 1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400)
GROUP BY bt.analysis_id, seg.bt_id, seg.street_name, seg.direction, seg.from_intersection, seg.to_intersection
ORDER BY analysis_id

--Ratio for bt obs
CREATE OR REPLACE VIEW jchew.ratio_bt_top10_1month AS
WITH X AS (
SELECT a.analysis_id AS analysis_id_1, a.bt_id AS bt_id_1, 
    b.analysis_id AS analysis_id_2, b.bt_id AS bt_id_2, a.street_name, 
    a.direction AS eb_nb, b.direction AS wb_sb, 
    a.from_intersection AS intersection_1, a.to_intersection AS intersection_2,
    a.sum_bt_obs AS eb_nb_obs, b.sum_bt_obs AS wb_sb_obs
    FROM jchew.bt_top10_1month a
JOIN jchew.bt_top10_1month b
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

--Create view for here obs
CREATE MATERIALIZED VIEW jchew.here_top10_1month AS
SELECT a.analysis_id, a.street_name, a.direction, 
a.from_intersection_name AS from_intersection, a.to_intersection_name AS to_intersection,
SUM(b.total) AS sum_here_obs, SUM(b.length) AS sum_length
FROM
(SELECT analysis_id, street_name, direction, 
 from_intersection_name, to_intersection_name, pp_link_dir AS link_dir, reference_length
FROM jchew.validation_bt_here
WHERE analysis_id IN (1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 1453464, 1453483, 1454196, 1454209, 
                      1454352, 1454366, 1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400)
) a
LEFT JOIN
(SELECT link_dir, count(tx) AS total, length FROM here.ta
WHERE tx BETWEEN '2019-11-01 00:00:00' AND '2019-11-30 23:59:00'
GROUP BY link_dir,length) b
USING (link_dir)
GROUP BY analysis_id, street_name, direction, from_intersection_name, to_intersection_name

--ratio for here obs
CREATE OR REPLACE VIEW jchew.ratio_here_top10_1month AS
WITH X AS (
SELECT a.analysis_id AS analysis_id_1, 
    b.analysis_id AS analysis_id_2, a.street_name,
    a.direction AS eb_nb, b.direction AS wb_sb, 
    a.from_intersection AS intersection_1, a.to_intersection AS intersection_2,
    a.sum_here_obs AS eb_nb_obs, b.sum_here_obs AS wb_sb_obs
    FROM jchew.here_top10_1month a
JOIN jchew.here_top10_1month b
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

--to compare
SELECT b.analysis_id_1, b.analysis_id_2, b.street_name, 
b.eb_nb, b.wb_sb, b.intersection_1, b.intersection_2, 
a.eb_nb_obs AS bt_eb_nb_obs, a.wb_sb_obs AS bt_wb_sb_obs,
b.eb_nb_obs AS here_eb_nb_obs, b.wb_sb_obs AS here_wb_sb_obs,
a."EB/WB or NB/SB Ratio" AS bt_ratio,
b."EB/WB or NB/SB Ratio" AS here_ratio,
a."Bias Towards" AS bt_bias,
b."Bias Towards" AS here_bias
FROM jchew.ratio_bt_top10_1month a
RIGHT JOIN jchew.ratio_here_top10_1month b
USING (analysis_id_1)