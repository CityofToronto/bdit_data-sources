CREATE TABLE IF NOT EXISTS gis.traffic_signal
(
    px text COLLATE pg_catalog."default" NOT NULL,
    main_street text COLLATE pg_catalog."default",
    midblock_route text COLLATE pg_catalog."default",
    side1_street text COLLATE pg_catalog."default",
    side2_street text COLLATE pg_catalog."default",
    private_access text COLLATE pg_catalog."default",
    additional_info text COLLATE pg_catalog."default",
    x numeric,
    y numeric,
    latitude numeric,
    longitude numeric,
    activationdate timestamp without time zone,
    signalsystem text COLLATE pg_catalog."default",
    non_system text COLLATE pg_catalog."default",
    control_mode text COLLATE pg_catalog."default",
    pedwalkspeed text COLLATE pg_catalog."default",
    aps_operation text COLLATE pg_catalog."default",
    numberofapproaches text COLLATE pg_catalog."default",
    objectid integer,
    geo_id integer,
    node_id integer,
    audiblepedsignal numeric,
    aps_activation_date timestamp without time zone,
    transit_preempt numeric,
    fire_preempt numeric,
    rail_preempt numeric,
    mi_prinx integer,
    geom geometry,
    bicycle_signal numeric,
    ups numeric,
    led_blankout_sign numeric,
    leading_pedestrian_intervals integer,
    lpi_north_implementation_date timestamp without time zone,
    lpi_south_implementation_date timestamp without time zone,
    lpi_east_implementation_date timestamp without time zone,
    lpi_west_implementation_date timestamp without time zone,
    lpi_comment text COLLATE pg_catalog."default",
    aps_activation_date timestamp without time zone,
    leading_pedestrian_intervals integer,
    CONSTRAINT _traffic_signal_pkey PRIMARY KEY (px)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS gis.traffic_signal OWNER TO gis_admins;

GRANT SELECT ON TABLE gis.traffic_signal TO bdit_humans;

GRANT ALL ON TABLE gis.traffic_signal TO gis_admins;

GRANT ALL ON TABLE gis.traffic_signal TO vz_api_bot;

COMMENT ON TABLE gis.traffic_signal
    IS 'Updated daily by the assets_pull dag https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/assets';

-- Index: traffic_signal_gix

-- DROP INDEX IF EXISTS gis.traffic_signal_gix;

CREATE INDEX IF NOT EXISTS traffic_signal_gix ON gis.traffic_signal USING gist(geom)
TABLESPACE pg_default;

-- Trigger: audit_trigger_row

-- DROP TRIGGER IF EXISTS audit_trigger_row ON gis.traffic_signal;

CREATE TRIGGER audit_trigger_row
AFTER INSERT OR DELETE OR UPDATE 
ON gis.traffic_signal
FOR EACH ROW
EXECUTE PROCEDURE gis.if_modified_func('true');

-- Trigger: audit_trigger_stm

-- DROP TRIGGER IF EXISTS audit_trigger_stm ON gis.traffic_signal;

CREATE TRIGGER audit_trigger_stm
AFTER TRUNCATE
ON gis.traffic_signal
FOR EACH STATEMENT
EXECUTE PROCEDURE gis.if_modified_func('true');
