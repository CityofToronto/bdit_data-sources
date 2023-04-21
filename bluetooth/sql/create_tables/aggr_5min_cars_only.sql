-- Materialized View: bluetooth.aggr_5min_cars_only

DROP MATERIALIZED VIEW bluetooth.aggr_5min_cars_only CASCADE;

CREATE MATERIALIZED VIEW bluetooth.aggr_5min_cars_only AS
SELECT
    bt_segments.analysis_id,
    '1970-01-01 00:00:00'::timestamp without time zone
    + '00:00:01'::interval
    * (
        floor(
            (date_part('epoch'::text, rs.measured_timestamp) - 1::double precision)
            / 300::double precision
        )
        * 300::double precision
    ) AS datetime_bin,
    percentile_cont(0.5::double precision) WITHIN GROUP (
        ORDER BY (rs.measured_time::double precision)
    ) AS tt,
    count(rs.user_id) AS obs
FROM bluetooth.observations AS rs
JOIN bluetooth.class_of_device USING (cod)
JOIN king_pilot.bt_segments USING (analysis_id)
WHERE
    rs.outlier_level = 0
    AND rs.device_class = 1
    AND (
        class_of_device.device_type::text
        = ANY(ARRAY['Hands-free Device'::character varying::text, 'WiFi'::character varying::text])
    )
    AND measured_timestamp >= '2017-09-01' AND measured_timestamp < '2018-02-01'
GROUP BY
    bt_segments.analysis_id,
    (
        floor(
            (date_part('epoch'::text, rs.measured_timestamp) - 1::double precision)
            / 300::double precision
        )
        * 300::double precision
    )
WITH DATA;

ALTER TABLE bluetooth.aggr_5min_cars_only
OWNER TO rdumas;
GRANT ALL ON TABLE bluetooth.aggr_5min_cars_only TO rdumas;
GRANT SELECT ON TABLE bluetooth.aggr_5min_cars_only TO bdit_humans;
