CREATE TABLE IF NOT EXISTS vds.veh_length_15min (
    uid bigserial,
    division_id smallint, 
    vds_id integer,
    datetime_15min timestamp,
    length_meter smallint,
    count smallint,
    total_count smallint,
    PRIMARY KEY uid, 
    UNIQUE (division_id, vds_id, datetime_15min, length_meter)
);

ALTER TABLE vds.veh_length_15min OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.veh_length_15min TO vds_bot;
GRANT ALL ON SEQUENCE vds.veh_length_15min_uid_seq TO vds_bot;

COMMENT ON TABLE vds.veh_length_15min IS 'A count of vehicle lengths from `raw_vdsvehicledata` 
aggregated to detector / lengths floored to 1m.';

-- DROP INDEX IF EXISTS vds.ix_veh_lengths_divid_dt;
CREATE INDEX IF NOT EXISTS ix_veh_lengths_divid_dt
ON vds.veh_length_15min
USING btree (
    division_id ASC NULLS LAST,
    datetime_15min ASC NULLS LAST
);

-- DROP INDEX IF EXISTS vds.ix_veh_lengths_divid_vdsid_dt;
CREATE INDEX IF NOT EXISTS ix_veh_lengths_divid_vdsid_dt
ON vds.veh_length_15min
USING btree (
    division_id ASC NULLS LAST,
    vds_id ASC NULLS LAST,
    datetime_15min ASC NULLS LAST
);