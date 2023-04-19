-- Materialized View: bluetooth.aggr_5min_i95

DROP MATERIALIZED VIEW bluetooth.aggr_5min_i95;

CREATE MATERIALIZED VIEW bluetooth.aggr_5min_i95 AS 
WITH temp_aggr AS (
    SELECT
        obs_1.analysis_id,
        date_trunc('hour'::text, obs_1.measured_timestamp) + trunc(date_part('minute'::text, obs_1.measured_timestamp) / 5::double precision) * '00:05:00'::interval AS datetime_bin,
        avg(obs_1.measured_time) OVER w AS travel_time,
        stddev(obs_1.measured_time) OVER w AS std_time,
        obs_1.measured_time
    FROM bluetooth.observations obs_1
    WHERE obs_1.outlier_level = 0 AND measured_time > 0
    WINDOW w AS (PARTITION BY obs_1.analysis_id, (date_trunc('hour'::text, obs_1.measured_timestamp) + trunc(date_part('minute'::text, obs_1.measured_timestamp) / 5::double precision) * '00:05:00'::interval))
)

SELECT obs.analysis_id,
    obs.datetime_bin,
    avg(obs.measured_time) AS travel_time,
    stddev(obs.measured_time) AS std_time,
    count(1) AS obs
FROM temp_aggr obs
WHERE 
    obs.measured_time::numeric > (obs.travel_time - 1.5 * obs.std_time) 
    AND obs.measured_time::numeric < (obs.travel_time + 1.5 * obs.std_time)
GROUP BY obs.analysis_id, obs.datetime_bin
WITH DATA;

ALTER TABLE bluetooth.aggr_5min_i95
  OWNER TO rdumas;
GRANT ALL ON TABLE bluetooth.aggr_5min_i95 TO rdumas;
GRANT SELECT ON TABLE bluetooth.aggr_5min_i95 TO bdit_humans WITH GRANT OPTION;
