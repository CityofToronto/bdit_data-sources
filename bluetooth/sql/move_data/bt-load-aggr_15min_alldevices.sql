TRUNCATE bluetooth.aggr_15min_alldevices;

INSERT INTO bluetooth.aggr_15min_alldevices (analysis_id, datetime_bin, tt, obs)
SELECT
    a.analysis_id,
    timestamp without time zone 'epoch'
    + interval '1 second' * (floor((extract('epoch' FROM a.datetime_bin)) / 900) * 900) AS datetime_bin,
    AVG(a.tt) AS tt,
    SUM(a.obs) AS obs


FROM bluetooth.aggr_5min_alldevices AS a
GROUP BY a.analysis_id, (floor((extract('epoch' FROM a.datetime_bin)) / 900) * 900)
ORDER BY a.analysis_id, (floor((extract('epoch' FROM a.datetime_bin)) / 900) * 900);