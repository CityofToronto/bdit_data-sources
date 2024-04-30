-- DROP TABLE IF EXISTS vds.config_comms_device;

CREATE TABLE IF NOT EXISTS vds.config_comms_device
(
    division_id smallint NOT NULL,
    fss_id integer NOT NULL,
    source_id character varying(5000) COLLATE pg_catalog."default" NOT NULL,
    start_timestamp timestamp without time zone NOT NULL,
    end_timestamp timestamp without time zone,
    has_gps_unit boolean NOT NULL,
    device_type smallint NOT NULL,
    description character varying(10000) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT commdeviceconfig_pkey PRIMARY KEY (start_timestamp, division_id, fss_id)
)
WITH (
    oids = FALSE
)
TABLESPACE pg_default;

ALTER TABLE vds.config_comms_device OWNER TO vds_admins;
GRANT INSERT, SELECT, UPDATE ON TABLE vds.config_comms_device TO vds_bot;
GRANT SELECT ON TABLE vds.config_comms_device TO bdit_humans;

COMMENT ON TABLE vds.config_comms_device IS
'Store raw data pulled from ITS Central `config_comms_device` table.
This table is useful for determing which technology is used by a RESCU sensor.
Join `vdsconfig.fss_id` to `config_comms_device.fss_id`. 
Note there may be duplicates on division_id+fss_id corresponding to updated locations/details over time.';

COMMENT ON COLUMN vds.config_comms_device.fss_id
IS 'Field renamed to `fss_id` to match `vdsconfig` table. `deviceid` in ITSC.';

COMMENT ON COLUMN vds.config_comms_device.source_id
IS 'This text field can help identify Wavetronix/Smartmicro sensor technology.';

CREATE INDEX IF NOT EXISTS ix_vdscommsdevice
ON vds.config_comms_device
USING btree (
    division_id ASC NULLS LAST,
    fss_id ASC NULLS LAST,
    start_timestamp ASC NULLS LAST,
    end_timestamp ASC NULLS LAST
);
