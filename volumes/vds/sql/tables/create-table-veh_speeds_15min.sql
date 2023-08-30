CREATE TABLE IF NOT EXISTS vds.veh_speeds_15min (
    uid bigserial PRIMARY KEY,
    division_id smallint, 
    vdsconfig_uid integer REFERENCES vds.vdsconfig(uid),
    entity_location_uid integer REFERENCES vds.entity_locations(uid),
    datetime_15min timestamp,
    speed_5kph smallint,
    count smallint,
    total_count smallint,
    UNIQUE (division_id, vdsconfig_uid, datetime_15min, speed_5kph)
);

ALTER TABLE vds.veh_speeds_15min OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.veh_speeds_15min TO vds_bot;
GRANT ALL ON SEQUENCE vds.veh_speeds_15min_uid_seq TO vds_bot;
GRANT SELECT ON TABLE vds.veh_speeds_15min TO bdit_humans;

COMMENT ON TABLE vds.veh_speeds_15min IS 'A count of vehicle speeds from `raw_vdsvehicledata` 
aggregated to detector / speeds floored to 5kph.';

-- DROP INDEX IF EXISTS vds.ix_veh_speeds_dt;
CREATE INDEX IF NOT EXISTS ix_veh_speeds_dt
ON vds.veh_speeds_15min
USING brin(datetime_15min);

-- DROP INDEX IF EXISTS vds.ix_veh_speeds_vdsconfiguid_dt;
CREATE INDEX IF NOT EXISTS ix_veh_speeds_vdsconfiguid_dt
ON vds.veh_speeds_15min
USING btree(
    vdsconfig_uid ASC nulls last,
    datetime_15min ASC nulls last -- noqa: PRS
);

-- DROP INDEX IF EXISTS vds.ix_veh_speeds_entity_location_uid;
CREATE INDEX IF NOT EXISTS ix_veh_speeds_entity_location_uid
ON vds.veh_speeds_15min
USING btree(
    entity_location_uid ASC nulls last
);