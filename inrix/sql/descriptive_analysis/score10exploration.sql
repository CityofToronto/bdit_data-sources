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
ORDER BY tmc, date_part, min



SELECT *
FROM inrix.raw_data201208

WHERE score = 10
LIMIT 100