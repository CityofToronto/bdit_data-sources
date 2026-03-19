CREATE TABLE IF NOT EXISTS ptc.open_data_summary (
    dt date,
    reported_trips_started bigint,
    trips_started_wav smallint,
    trips_started_nonwav bigint,
    trips_started_pooled bigint,
    trips_cancelled_driver_wav smallint,
    trips_cancelled_driver_nonwav bigint,
    trips_cancelled_passenger_wav smallint,
    trips_cancelled_passenger_nonwav bigint,
    dist_avg numeric,
    fare_avg numeric,
    waittime_wav_avg numeric,
    waittime_nonwav_avg numeric,
    active_vehicles bigint,
    time_available numeric,
    time_enroute numeric,
    time_waiting numeric,
    time_ontrip numeric,
    dist_available_routed numeric,
    dist_enroute_routed numeric,
    dist_ontrip_routed numeric,
    dist_ontrip_reported numeric,
    percent_time_available numeric,
    percent_time_enroute numeric,
    percent_time_waiting numeric,
    percent_time_ontrip numeric,
    percent_dist_available_routed numeric,
    percent_dist_enroute_routed numeric,
    percent_dist_ontrip_routed numeric,
    CONSTRAINT open_data_summ PRIMARY KEY (dt)
);

ALTER TABLE IF EXISTS ptc.open_data_summary
OWNER TO ptc_admins;
GRANT ALL ON TABLE ptc.open_data_summary TO ptc_admins;

REVOKE ALL ON TABLE ptc.open_data_summary FROM bdit_humans;
GRANT SELECT ON TABLE ptc.open_data_summary TO bdit_humans;

GRANT SELECT, INSERT, DELETE ON TABLE ptc.open_data_summary TO ptc_bot;
