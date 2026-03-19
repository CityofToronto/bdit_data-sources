CREATE TABLE ptc.open_data_operating_hours (
    vehid text,
    hr timestamp with time zone,
    reported_trips_started smallint,
    reported_trip_fractions numeric,
    pooled_trips_started smallint,
    pooled_trip_fractions numeric,
    routed_trips_started smallint,
    driver_cancelled_trips smallint,
    passenger_cancelled_trips smallint,
    toronto_boundary_crossed boolean,
    fares numeric,
    time_available numeric,
    time_enroute numeric,
    time_waiting numeric,
    time_ontrip numeric,
    dist_available_routed numeric,
    dist_enroute_routed numeric,
    dist_ontrip_routed numeric,
    dist_ontrip_reported numeric,
    CONSTRAINT veh_id_hr_pkey PRIMARY KEY (vehid, hr)
) PARTITION BY RANGE (hr);

ALTER TABLE IF EXISTS ptc.open_data_operating_hours
OWNER TO ptc_admins;

GRANT ALL ON TABLE ptc.open_data_operating_hours TO ptc_admins;

REVOKE ALL ON TABLE ptc.open_data_operating_hours FROM bdit_humans;
GRANT SELECT ON TABLE ptc.open_data_operating_hours TO bdit_humans;

GRANT SELECT, INSERT, DELETE ON TABLE ptc.open_data_operating_hours TO ptc_bot;

-- Partitions SQL
CREATE TABLE ptc.open_data_operating_hours_2020 PARTITION OF ptc.open_data_operating_hours
FOR VALUES FROM ('2020-01-01 00:00:00') TO ('2021-01-01 00:00:00')
TABLESPACE pg_default;
ALTER TABLE IF EXISTS ptc.open_data_operating_hours_2020
OWNER TO ptc_admins;

CREATE TABLE ptc.open_data_operating_hours_2021 PARTITION OF ptc.open_data_operating_hours
FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2022-01-01 00:00:00')
TABLESPACE pg_default;
ALTER TABLE IF EXISTS ptc.open_data_operating_hours_2021
OWNER TO ptc_admins;

CREATE TABLE ptc.open_data_operating_hours_2022 PARTITION OF ptc.open_data_operating_hours
FOR VALUES FROM ('2022-01-01 00:00:00') TO ('2023-01-01 00:00:00')
TABLESPACE pg_default;
ALTER TABLE IF EXISTS ptc.open_data_operating_hours_2022
OWNER TO ptc_admins;

CREATE TABLE ptc.open_data_operating_hours_2023 PARTITION OF ptc.open_data_operating_hours
FOR VALUES FROM ('2023-01-01 00:00:00') TO ('2024-01-01 00:00:00')
TABLESPACE pg_default;
ALTER TABLE IF EXISTS ptc.open_data_operating_hours_2023
OWNER TO ptc_admins;

CREATE TABLE ptc.open_data_operating_hours_2024 PARTITION OF ptc.open_data_operating_hours
FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2025-01-01 00:00:00')
TABLESPACE pg_default;
ALTER TABLE IF EXISTS ptc.open_data_operating_hours_2024
OWNER TO ptc_admins;

CREATE TABLE ptc.open_data_operating_hours_2025 PARTITION OF ptc.open_data_operating_hours
FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2026-01-01 00:00:00')
TABLESPACE pg_default;
ALTER TABLE IF EXISTS ptc.open_data_operating_hours_2025
OWNER TO ptc_admins;
