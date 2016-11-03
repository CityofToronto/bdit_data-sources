SELECT date_trunc('month', tx) as mon, score, COUNT(*)
FROM inrix.raw_data
WHERE (tx BETWEEN '2014-01-01'::date AND '2014-03-31'::DATE OR tx BETWEEN '2015-01-01'::date AND '2015-03-31'::DATE OR tx BETWEEN '2016-01-01'::date AND '2016-03-31'::DATE)
GROUP BY score, date_trunc('month', tx)
ORDER BY mon, score