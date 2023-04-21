/*All TMCs*/
-- SELECT date_trunc('month', dt) as yyyymm, COUNT(DISTINCT tmc) as tmc_cnt, SUM(count) as score30rows
-- FROM inrix.agg_extract_hour 
-- GROUP BY date_trunc('month', dt);

/*Just Toronto*/
SELECT
    date_trunc('month', dt) AS yyyymm,
    COUNT(DISTINCT tmc) AS tmc_cnt,
    SUM(count) AS score30rows
FROM inrix.agg_extract_hour
INNER JOIN gis.inrix_tmc_tor USING (tmc)
GROUP BY date_trunc('month', dt) ORDER BY yyyymm