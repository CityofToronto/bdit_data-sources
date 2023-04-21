-- Materialized View: bluetooth.bt_aggr_201606

-- DROP MATERIALIZED VIEW bluetooth.bt_aggr_201606;

CREATE MATERIALIZED VIEW bluetooth.aggr_5min_i95 AS
WITH temp_aggr AS (
    SELECT
        obs_1.analysis_id,
        date_trunc('hour'::text, obs_1.measured_timestamp)
        + trunc(date_part('minute'::text, obs_1.measured_timestamp) / 5::double precision)
        * '00:05:00'::interval AS datetime_bin,
        avg(obs_1.measured_time) OVER w AS travel_time,
        stddev(obs_1.measured_time) OVER w AS std_time,
        measured_time
    FROM bluetooth.observations AS obs_1
    WHERE obs_1.outlier_level = 0
    WINDOW
        w AS (
            PARTITION BY
                obs_1.analysis_id,
                date_trunc('hour'::text, obs_1.measured_timestamp)
                + trunc(date_part('minute'::text, obs_1.measured_timestamp) / 5::double precision)
                * '00:05:00'::interval
        )
)

SELECT
    obs.analysis_id,
    datetime_bin,
    avg(measured_time) AS travel_time,
    stddev(measured_time) AS std_time,
    count(1) AS obs
FROM temp_aggr AS obs
WHERE
    measured_time::numeric > (travel_time - 1.5 * std_time)
    AND measured_time::numeric < (travel_time + 1.5 * std_time)
GROUP BY obs.analysis_id, datetime_bin

WITH DATA;

