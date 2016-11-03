CREATE SEQUENCE score_block_id;

WITH tmcs AS (SELECT * FROM (VALUES ('C09+04378'),('C09+04381'),('C09+04392'),('C09+04393'),('C09+04568'),('C09+04569'),('C09+04570'),('C09+04571'),('C09+04573'),('C09+04574') ) tmcs(tmc))

SELECT tmc, tx, speed, CASE WHEN LEAD(tx) OVER w - tx > interval '1 min' THEN nextval('score_block_id') ELSE currval('score_block_id') END as score_block_id
INTO TEMP inrix201208
FROM inrix.raw_data201208
NATURAL JOIN tmcs
WHERE score = 10
WINDOW w AS (PARTITION BY tmc ORDER BY tx)
ORDER BY tmc, tx;
DROP SEQUENCE score_block_id;

SELECT tmc,extract('isodow' FROM tx), min(tx)::TIME, MAX(tx)::TIME, speed
FROM inrix201208
GROUP BY tmc,speed, extract('isodow' FROM tx)
ORDER BY tmc, date_part, min;

SELECT *
FROM inrix.raw_data201208

WHERE score = 10
LIMIT 100

INSERT INTO rdumas.inrix_score10
SELECT DISTINCT tmc, speed, date_trunc('month', tx) as mon
FROM inrix.raw_data
-- WHERE score = 10 AND (tx BETWEEN '2014-01-01'::date AND '2014-03-31'::DATE OR tx BETWEEN '2015-01-01'::date AND '2015-03-31'::DATE);
WHERE score = 10 AND (tx BETWEEN '2016-01-01'::date AND '2016-03-31'::DATE);


SELECT tmc, date_trunc('day', tx)::DATE as dt, extract('hour' from tx)::smallint as hh, score, count(*)
INTO rdumas.inrix_score_counts
FROM inrix.raw_data
WHERE (tx BETWEEN '2016-01-01'::date AND '2016-03-31'::DATE)
GROUP BY tmc, dt, hh, score;


/*See which score 10 values change over time. Spoiler: None of them*/
-- WITH tmcs AS (SELECT tmc, COUNT (DISTINCT speed) as cnt
-- FROM rdumas.inrix_score10
-- GROUP BY tmc)
-- SELECT i.* 
-- FROM rdumas.inrix_score10 i
-- natural JOIN tmcs
-- WHERE cnt > 1
-- ORDER BY tmc, mon;

SELECT mon, SUM(CASE WHEN  s.speed != tmc.speed THEN 1 ELSE 0 END) "No of TMCs where speeds don't match ref" , COUNT(DISTINCT tmc) AS "Distinct TMCs"

--  tmc.speed as "TMC ref speed", s.speed AS "Score 10 Speed"
FROM gis.inrix_tmc_tor tmc
INNER JOIN inrix_score10 s USING(tmc)
GROUP BY mon 
ORDER BY mon

SELECT avg(s.speed - tmc.speed), percentile_cont(0.5) WITHIN GROUP (ORDER BY s.speed - tmc.speed)

--  tmc.speed as "TMC ref speed", s.speed AS "Score 10 Speed"
FROM gis.inrix_tmc_tor tmc
INNER JOIN inrix_score10 s USING(tmc)
WHERE mon = '2013-01-01'::DATE

SELECT tmc, COUNT(1)
FROM inrix_