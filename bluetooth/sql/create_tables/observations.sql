CREATE TABLE IF NOT EXISTS bluetooth.observations (
    id bigserial NOT NULL PRIMARY KEY,
    user_id bigint,
    analysis_id integer,
    measured_time integer,
    measured_time_no_filter integer,
    startpoint_number smallint,
    startpoint_name character varying(8),
    endpoint_number smallint,
    endpoint_name character varying(8),
    measured_timestamp timestamp without time zone,
    outlier_level smallint,
    cod bigint,
    device_class smallint
);
