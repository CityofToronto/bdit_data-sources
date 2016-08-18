
CREATE TABLE IF NOT EXISTS inrix.agg_extract_hour201301 ()INHERITS(inrix.agg_extract_hour);


INSERT INTO inrix.agg_extract_hour201301
SELECT

    tmc, 
	extract(hour from tx)*10 + trunc(extract(minute from tx)/15)+1 AS time_15_continuous, 
    tx::DATE as dt,
	COUNT(speed) AS cnt, 
	AVG(speed) AS avg_speed

FROM inrix.raw_data201301
WHERE score = 30
GROUP BY tmc, tx::date, time_15_continuous
