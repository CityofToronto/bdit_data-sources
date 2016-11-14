SELECT date_trunc('month', tx) as mon, score, COUNT(*)
FROM inrix.raw_data
WHERE (tx BETWEEN '2014-04-01'::date AND '2014-12-31'::DATE OR tx BETWEEN '2015-04-01'::date AND '2015-12-31'::DATE OR tx BETWEEN '2016-04-01'::date AND '2016-06-30'::DATE)
GROUP BY score, date_trunc('month', tx)
ORDER BY mon, score