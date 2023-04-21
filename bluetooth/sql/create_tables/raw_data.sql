CREATE TABLE bluetooth.raw_data
(
    user_id bigint,
    analysis_id integer,
    measured_time integer,
    measured_time_no_filter integer,
    startpoint_number smallint,
    startpoint_name character varying(5),
    endpoint_number smallint,
    endpoint_name character varying(5),
    measured_timestamp timestamp without time zone,
    outlier_level smallint,
    cod bigint,
    device_class smallint
)
WITH (
    oids = TRUE
);
ALTER TABLE bluetooth.raw_data
OWNER TO aharpal;
GRANT ALL ON TABLE bluetooth.raw_data TO aharpal;
GRANT ALL ON TABLE bluetooth.raw_data TO public;