DROP TABLE IF EXISTS gardiner_bins;
DROP TABLE IF EXISTS lakeshore_bins;
DROP TABLE IF EXISTS bluetooth.aggr_15min_temp;

CREATE TEMPORARY TABLE gardiner_bins (
    datetime_bin timestamp without time zone
);

CREATE TEMPORARY TABLE lakeshore_bins (
    datetime_bin timestamp without time zone
);

CREATE TABLE bluetooth.aggr_15min_temp (
    analysis_id bigint,
    datetime_bin timestamp without time zone,
    tt numeric,
    obs integer
);

INSERT INTO gardiner_bins
SELECT a.datetime_bin
FROM bluetooth.aggr_15min AS a
WHERE a.analysis_id IN (116749, 1387517, 1388022, 130557, 1391996)
GROUP BY a.datetime_bin
HAVING COUNT(a . *) = 5;


INSERT INTO lakeshore_bins
SELECT a.datetime_bin
FROM bluetooth.aggr_15min AS a
WHERE a.analysis_id IN (1393119, 1396036, 1396053)
GROUP BY a.datetime_bin
HAVING COUNT(a . *) = 3;

INSERT INTO bluetooth.aggr_15min_temp
SELECT
    analysis_id,
    datetime_bin,
    tt,
    obs
FROM bluetooth.aggr_15min
WHERE
    analysis_id IN (116749, 1387517, 1388022, 130557, 1391996) AND datetime_bin IN (SELECT datetime_bin FROM gardiner_bins);

INSERT INTO bluetooth.aggr_15min_temp
SELECT
    analysis_id,
    datetime_bin,
    tt,
    obs
FROM bluetooth.aggr_15min
WHERE
    analysis_id IN (1393119, 1396036, 1396053) AND datetime_bin IN (SELECT datetime_bin FROM lakeshore_bins);
