--Aggregating score counts by tmc, Month, dow, and hour

INSERT INTO rdumas.inrix_score_counts2
SELECT tmc, date_trunc('month', tx) as mon, extract('isodow' from tx)::smallint as isodow, extract('hour' from tx)::smallint as hh, score, count(1)

  FROM inrix.raw_data
-- WHERE (tx BETWEEN '2015-04-01'::date AND '2015-12-31'::DATE) OR (tx BETWEEN '2013-04-01'::date AND '2013-06-30'::DATE) OR (tx BETWEEN '2012-07-01'::date AND '2012-12-31'::DATE) OR (tx BETWEEN '2016-04-01'::date AND '2016-06-30'::DATE)
WHERE (tx BETWEEN '2014-04-01'::date AND '2014-12-31'::DATE)

  GROUP BY tmc, mon, isodow, hh, score
