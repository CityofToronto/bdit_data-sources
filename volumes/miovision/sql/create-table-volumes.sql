-- Table: miovision_api.volumes

-- DROP TABLE miovision_api.volumes;

CREATE TABLE miovision_api.volumes
(
  volume_uid serial NOT NULL,
  intersection_uid integer,
  datetime_bin timestamp without time zone,
  classification_uid integer,
  leg text,
  movement_uid integer,
  volume integer,
  volume_15min_tmc_uid integer,
  CONSTRAINT volumes_volumes_15min_tmc_uid_fkey FOREIGN KEY (volume_15min_tmc_uid)
      REFERENCES miovision_api.volumes_15min_tmc (volume_15min_tmc_uid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE SET NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE miovision_api.volumes
  OWNER TO miovision_admins;
GRANT ALL ON TABLE miovision_api.volumes TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.volumes TO aharpal;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes TO bdit_humans WITH GRANT OPTION;
GRANT UPDATE, INSERT, DELETE ON TABLE miovision_api.volumes TO rliu;

-- Index: miovision_api.volumes_datetime_bin_idx

-- DROP INDEX miovision_api.volumes_datetime_bin_idx;

CREATE INDEX volumes_datetime_bin_idx
  ON miovision_api.volumes
  USING btree
  (datetime_bin);

-- Index: miovision_api.volumes_intersection_uid_classification_uid_leg_movement_ui_idx

-- DROP INDEX miovision_api.volumes_intersection_uid_classification_uid_leg_movement_ui_idx;

CREATE INDEX volumes_intersection_uid_classification_uid_leg_movement_ui_idx
  ON miovision_api.volumes
  USING btree
  (intersection_uid, classification_uid, leg COLLATE pg_catalog."default", movement_uid);

-- Index: miovision_api.volumes_intersection_uid_idx

-- DROP INDEX miovision_api.volumes_intersection_uid_idx;

CREATE INDEX volumes_intersection_uid_idx
  ON miovision_api.volumes
  USING btree
  (intersection_uid);

-- Index: miovision_api.volumes_volume_15min_tmc_uid_idx

-- DROP INDEX miovision_api.volumes_volume_15min_tmc_uid_idx;

CREATE INDEX volumes_volume_15min_tmc_uid_idx
  ON miovision_api.volumes
  USING btree
  (volume_15min_tmc_uid);

CREATE OR REPLACE FUNCTION miovision_api.volumes_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
	IF new.datetime_bin >= '2018-01-01'::date AND new.datetime_bin < ('2018-01-01'::date + '1 year'::interval) THEN INSERT INTO miovision_api.volumes_2018 (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume, volume_15min_tmc_uid) VALUES (NEW.*);
	ELSIF new.datetime_bin >= '2019-01-01'::date AND new.datetime_bin < ('2019-01-01'::date + '1 year'::interval) THEN INSERT INTO miovision_api.volumes_2019 (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume, volume_15min_tmc_uid) VALUES (NEW.*);
	ELSIF new.datetime_bin >= '2020-01-01'::date AND new.datetime_bin < ('2020-01-01'::date + '1 year'::interval) THEN INSERT INTO miovision_api.volumes_2020 (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume, volume_15min_tmc_uid) VALUES (NEW.*);
  ELSE 
    RAISE EXCEPTION 'Datetime_bin out of range.  Fix the volumes_insert_trigger() function!';
	END IF;
	RETURN NULL;
END;
$$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS insert_trigger ON miovision_api.volumes; 
CREATE TRIGGER insert_trigger BEFORE INSERT ON miovision_api.volumes
		  FOR EACH ROW EXECUTE PROCEDURE miovision_api.volumes_insert_trigger();
/*Create annual table partitions*/

DO $do$
DECLARE
	startdate DATE;
	yyyy TEXT;
	basetablename TEXT := 'volumes_';
	tablename TEXT;
BEGIN

	for yyyy IN 2018..2020 LOOP
			startdate:= to_date(yyyy||'-01-01', 'YYYY-MM-DD');
			tablename:= basetablename||yyyy;
			EXECUTE format($$DROP TABLE IF EXISTS miovision_api.%I CASCADE; CREATE TABLE miovision_api.%I 
				(CHECK (datetime_bin >= DATE '$$||startdate ||$$'AND datetime_bin < DATE '$$||startdate ||$$'+ INTERVAL '1 year')
        ,FOREIGN KEY (volume_15min_tmc_uid) REFERENCES miovision_api.volumes_15min_tmc (volume_15min_tmc_uid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE SET NULL
				 --, UNIQUE(intersection_uid, datetime_bin, classification_uid, leg, movement_uid)
				) INHERITS (miovision_api.volumes)$$
				, tablename, tablename);
			EXECUTE format($$ALTER TABLE miovision_api.%I OWNER TO miovision_admins$$, tablename);
      EXECUTE format($$ CREATE INDEX ON miovision_api.%I USING brin(datetime_bin) $$, tablename);
      EXECUTE format($$ CREATE INDEX ON miovision_api.%I (volume_15min_tmc_uid) $$, tablename);
      EXECUTE format($$ CREATE INDEX ON miovision_api.%I (intersection_uid) $$, tablename);
	END LOOP;
END;
$do$ LANGUAGE plpgsql

