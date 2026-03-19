-- DROP TABLE IF EXISTS ptc.open_data_trips;

CREATE TABLE IF NOT EXISTS ptc.open_data_trips (
    dt date,
    pickup_hr timestamp with time zone,
    pickup_municipality text,
    pickup_community_council text,
    pickup_ward text,
    dropoff_municipality text,
    dropoff_community_council text,
    dropoff_ward text,
    trips_total smallint,
    fare_avg numeric,
    distance_avg numeric,
    waittime_avg numeric,
    duration_avg numeric,
    CONSTRAINT open_data_trips_unique UNIQUE (
        dt, pickup_hr, pickup_municipality, pickup_community_council, pickup_ward,
        dropoff_municipality, dropoff_community_council, dropoff_ward
    )
) PARTITION BY RANGE (dt);

ALTER TABLE IF EXISTS ptc.open_data_trips
OWNER TO ptc_admins;
GRANT ALL ON TABLE ptc.open_data_trips TO ptc_admins;

REVOKE ALL ON TABLE ptc.open_data_trips FROM bdit_humans;
GRANT SELECT ON TABLE ptc.open_data_trips TO bdit_humans;

GRANT SELECT, INSERT, DELETE ON TABLE ptc.open_data_trips TO ptc_bot;

-- Partitions SQL

CREATE TABLE ptc.open_data_trips_2016 PARTITION OF ptc.open_data_trips
FOR VALUES FROM ('2016-01-01') TO ('2017-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS ptc.open_data_trips_2016
OWNER to ptc_admins;
CREATE TABLE ptc.open_data_trips_2017 PARTITION OF ptc.open_data_trips
FOR VALUES FROM ('2017-01-01') TO ('2018-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS ptc.open_data_trips_2017
OWNER to ptc_admins;
CREATE TABLE ptc.open_data_trips_2018 PARTITION OF ptc.open_data_trips
FOR VALUES FROM ('2018-01-01') TO ('2019-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS ptc.open_data_trips_2018
OWNER to ptc_admins;
CREATE TABLE ptc.open_data_trips_2019 PARTITION OF ptc.open_data_trips
FOR VALUES FROM ('2019-01-01') TO ('2020-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS ptc.open_data_trips_2019
OWNER to ptc_admins;
CREATE TABLE ptc.open_data_trips_2020 PARTITION OF ptc.open_data_trips
FOR VALUES FROM ('2020-01-01') TO ('2021-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS ptc.open_data_trips_2020
OWNER to ptc_admins;
CREATE TABLE ptc.open_data_trips_2021 PARTITION OF ptc.open_data_trips
FOR VALUES FROM ('2021-01-01') TO ('2022-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS ptc.open_data_trips_2021
OWNER to ptc_admins;
CREATE TABLE ptc.open_data_trips_2022 PARTITION OF ptc.open_data_trips
FOR VALUES FROM ('2022-01-01') TO ('2023-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS ptc.open_data_trips_2022
OWNER to ptc_admins;
CREATE TABLE ptc.open_data_trips_2023 PARTITION OF ptc.open_data_trips
FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS ptc.open_data_trips_2023
OWNER to ptc_admins;
CREATE TABLE ptc.open_data_trips_2024 PARTITION OF ptc.open_data_trips
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS ptc.open_data_trips_2024
OWNER to ptc_admins;
CREATE TABLE ptc.open_data_trips_2025 PARTITION OF ptc.open_data_trips
FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS ptc.open_data_trips_2025
OWNER to ptc_admins;
