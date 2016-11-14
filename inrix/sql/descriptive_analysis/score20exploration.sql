SELECT mon, score, sum(count)
FROM rdumas.inrix_score_counts2
INNER JOIN gis.inrix_tmc_tor USING (tmc)
WHERE tmc in (SELECT tmc FROM score20props) AND roadname IN ('Don Valley Pky','Gardiner Expy')
GROUP BY mon, score

SELECT tmc, count/ 3 as num_per_month, 100.0* count/(('2016-03-31'::DATE - '2016-01-01'::DATE) *24 * 60) AS pct_observations, 100.0* count / SUM(count) OVER () AS proportion
INTO rdumas.score20props
FROM score20_counts