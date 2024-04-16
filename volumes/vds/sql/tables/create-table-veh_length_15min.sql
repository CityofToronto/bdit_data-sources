CREATE TABLE IF NOT EXISTS vds.veh_length_15min (
    uid bigserial PRIMARY KEY,
    division_id smallint, 
    vdsconfig_uid integer REFERENCES vds.vdsconfig(uid),
    entity_location_uid integer REFERENCES vds.entity_locations(uid),
    datetime_15min timestamp,
    length_meter smallint,
    count smallint,
    total_count smallint,
    UNIQUE (division_id, vdsconfig_uid, datetime_15min, length_meter)
);

ALTER TABLE vds.veh_length_15min OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.veh_length_15min TO vds_bot;
GRANT ALL ON SEQUENCE vds.veh_length_15min_uid_seq TO vds_bot;
GRANT SELECT ON TABLE vds.veh_length_15min TO bdit_humans;

COMMENT ON TABLE vds.veh_length_15min IS 'A count of vehicle lengths from `raw_vdsvehicledata` 
aggregated to detector / lengths floored to 1m.';

-- DROP INDEX IF EXISTS vds.ix_veh_lengths_dt;
CREATE INDEX IF NOT EXISTS ix_veh_lengths_dt
ON vds.veh_length_15min
USING brin(datetime_15min);

-- DROP INDEX IF EXISTS vds.ix_veh_lengths_vdsconfiguid_dt;
CREATE INDEX IF NOT EXISTS ix_veh_lengths_vdsconfiguid_dt
ON vds.veh_length_15min
USING btree(
    vdsconfig_uid ASC nulls last,
    datetime_15min ASC nulls last -- noqa: PRS
);

-- DROP INDEX IF EXISTS vds.ix_veh_lengths_entity_location_uid;
CREATE INDEX IF NOT EXISTS ix_veh_lengths_entity_location_uid
ON vds.veh_length_15min
USING btree(
    entity_location_uid ASC nulls last
);
