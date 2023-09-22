-- Table: collisions_replicator.acc_safe_copy

-- DROP TABLE IF EXISTS collisions_replicator.acc_safe_copy;

CREATE TABLE IF NOT EXISTS collisions_replicator.acc_safe_copy
(
    "ACCNB" character varying(10) COLLATE pg_catalog."default",
    "RELACCNB" character varying(10) COLLATE pg_catalog."default",
    "ACCDATE" timestamp without time zone,
    "DAY_NO" character varying(1) COLLATE pg_catalog."default",
    "ACCTIME" character varying(4) COLLATE pg_catalog."default",
    "PATAREA" character varying(4) COLLATE pg_catalog."default",
    "STNAME1" character varying(35) COLLATE pg_catalog."default",
    "STREETYPE1" character varying(4) COLLATE pg_catalog."default",
    "DIR1" character varying(1) COLLATE pg_catalog."default",
    "STNAME2" character varying(35) COLLATE pg_catalog."default",
    "STREETYPE2" character varying(4) COLLATE pg_catalog."default",
    "DIR2" character varying(1) COLLATE pg_catalog."default",
    "STNAME3" character varying(35) COLLATE pg_catalog."default",
    "STREETYPE3" character varying(4) COLLATE pg_catalog."default",
    "DIR3" character varying(1) COLLATE pg_catalog."default",
    "PER_INV" character varying(2) COLLATE pg_catalog."default",
    "VEH_INV" character varying(2) COLLATE pg_catalog."default",
    "MUNICIPAL" character varying(2) COLLATE pg_catalog."default",
    "LOCCOORD" character varying(2) COLLATE pg_catalog."default",
    "IMPCTAREA" character varying(2) COLLATE pg_catalog."default",
    "ACCLASS" character varying(2) COLLATE pg_catalog."default",
    "ACCLOC" character varying(2) COLLATE pg_catalog."default",
    "TRAFFICTL" character varying(2) COLLATE pg_catalog."default",
    "DRIVAGE" character varying(2) COLLATE pg_catalog."default",
    "VEH_NO" character varying(2) COLLATE pg_catalog."default",
    "VEHTYPE" character varying(2) COLLATE pg_catalog."default",
    "TOWEDVEH" character varying(2) COLLATE pg_catalog."default",
    "INITDIR" character varying(2) COLLATE pg_catalog."default",
    "IMPACTYPE" character varying(2) COLLATE pg_catalog."default",
    "IMPLOC" character varying(2) COLLATE pg_catalog."default",
    "EVENT1" character varying(2) COLLATE pg_catalog."default",
    "EVENT2" character varying(2) COLLATE pg_catalog."default",
    "EVENT3" character varying(2) COLLATE pg_catalog."default",
    "PER_NO" character varying(2) COLLATE pg_catalog."default",
    "INVTYPE" character varying(2) COLLATE pg_catalog."default",
    "INVAGE" character varying(2) COLLATE pg_catalog."default",
    "INJURY" character varying(1) COLLATE pg_catalog."default",
    "SAFEQUIP" character varying(2) COLLATE pg_catalog."default",
    "DRIVACT" character varying(2) COLLATE pg_catalog."default",
    "DRIVCOND" character varying(2) COLLATE pg_catalog."default",
    "PEDCOND" character varying(2) COLLATE pg_catalog."default",
    "PEDACT" character varying(2) COLLATE pg_catalog."default",
    "CHARGE" character varying(5) COLLATE pg_catalog."default",
    "CHARGE2" character varying(5) COLLATE pg_catalog."default",
    "CHARGE3" character varying(5) COLLATE pg_catalog."default",
    "CHARGE4" character varying(5) COLLATE pg_catalog."default",
    "VISIBLE" character varying(2) COLLATE pg_catalog."default",
    "LIGHT" character varying(2) COLLATE pg_catalog."default",
    "RDSFCOND" character varying(2) COLLATE pg_catalog."default",
    "VEHIMPTYPE" character varying(2) COLLATE pg_catalog."default",
    "MANOEUVER" character varying(2) COLLATE pg_catalog."default",
    "ENTRY" character varying(1) COLLATE pg_catalog."default",
    "GEOCODE" character varying(6) COLLATE pg_catalog."default",
    "FACTOR_ERR" character varying(1) COLLATE pg_catalog."default",
    "REP_TYPE" character varying(1) COLLATE pg_catalog."default",
    "BADGE_NO" character varying(5) COLLATE pg_catalog."default",
    "POSTAL" character varying(7) COLLATE pg_catalog."default",
    "XCOORD" character varying(10) COLLATE pg_catalog."default",
    "YCOORD" character varying(10) COLLATE pg_catalog."default",
    "PRECISE_XY" character varying(1) COLLATE pg_catalog."default",
    "LONGITUDE" double precision,
    "LATITUDE" double precision,
    "CHANGED" smallint,
    "SENT_UNIT" character varying(12) COLLATE pg_catalog."default",
    "SENT_DATE" timestamp without time zone,
    "STATUS" character varying(12) COLLATE pg_catalog."default",
    "CITY_AREA" character varying(12) COLLATE pg_catalog."default",
    "USER_ID" character varying(12) COLLATE pg_catalog."default",
    "CRC_UNIT" character varying(12) COLLATE pg_catalog."default",
    "COMMENTS" character varying(250) COLLATE pg_catalog."default",
    "MTP_DIVISION" character varying(12) COLLATE pg_catalog."default",
    "POLICE_AGENCY" character varying(12) COLLATE pg_catalog."default",
    "SUBMIT_BADGE_NUMBER" character varying(10) COLLATE pg_catalog."default",
    "SUBMIT_DATE" timestamp without time zone,
    "BIRTHDATE" timestamp without time zone,
    "PRIVATE_PROPERTY" character varying(1) COLLATE pg_catalog."default",
    "PERSON_ID" bigint,
    "USERID" character varying(50) COLLATE pg_catalog."default",
    "TS" timestamp without time zone,
    "ROAD_CLASS" character varying(50) COLLATE pg_catalog."default",
    "SYMBOL_NUM" bigint,
    "ROTATION_NUM" bigint,
    "PX" character varying(10) COLLATE pg_catalog."default",
    "DISTRICT" character varying(30) COLLATE pg_catalog."default",
    "QUADRANT" character varying(5) COLLATE pg_catalog."default",
    "FAILTOREM" smallint,
    "YEAR" character varying(5) COLLATE pg_catalog."default",
    "REC_ID" bigint,
    "PEDTYPE" character varying(2) COLLATE pg_catalog."default",
    "CYCLISTYPE" character varying(2) COLLATE pg_catalog."default",
    "SIDEWALKCYCLE" character varying(10) COLLATE pg_catalog."default",
    "CYCACT" character varying(2) COLLATE pg_catalog."default",
    "CYCCOND" character varying(2) COLLATE pg_catalog."default",
    "MVAIMG" smallint,
    "WARDNUM" character varying(40) COLLATE pg_catalog."default",
    "FATAL_NO" double precision,
    "DESCRIPTION" character varying(4000) COLLATE pg_catalog."default",
    "TAB_REPORT" character varying(500) COLLATE pg_catalog."default",
    "ACTUAL_SPEED" smallint,
    "POSTED_SPEED" smallint,
    "TRAFCTLCOND" character varying(2) COLLATE pg_catalog."default",
    CONSTRAINT unique_rec_id UNIQUE ("REC_ID") -- the upsert query needs a unique id and this is it!
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS collisions_replicator.acc_safe_copy
    OWNER to collision_admins;

GRANT ALL ON TABLE collisions_replicator.acc_safe_copy TO collision_admins;

GRANT SELECT ON TABLE collisions_replicator.acc_safe_copy TO collisions_bot;

GRANT SELECT ON TABLE collisions_replicator.acc_safe_copy TO data_collection;

GRANT ALL ON TABLE collisions_replicator.acc_safe_copy TO rds_superuser WITH GRANT OPTION;

COMMENT ON TABLE collisions_replicator.acc_safe_copy
    IS 'Copy of the "ACC" table. The "ACC" table gets DROPped every time the data are refreshed';

-- Trigger: audit_trigger_row

-- DROP TRIGGER IF EXISTS audit_trigger_row ON collisions_replicator.acc_safe_copy;
-- This trigger makes it so that information about insert / delete / update changes to acc_safe_copy are tracked in collisions_replicator.logged_actions.

CREATE TRIGGER audit_trigger_row
    AFTER INSERT OR DELETE OR UPDATE 
    ON collisions_replicator.acc_safe_copy
    FOR EACH ROW
    EXECUTE FUNCTION collisions_replicator.if_modified_func('true');

-- Trigger: audit_trigger_stm

-- DROP TRIGGER IF EXISTS audit_trigger_stm ON collisions_replicator.acc_safe_copy;
-- This trigger makes it so that information about truncate changes to acc_safe_copy are tracked in collisions_replicator.logged_actions.

CREATE TRIGGER audit_trigger_stm
    AFTER TRUNCATE
    ON collisions_replicator.acc_safe_copy
    FOR EACH STATEMENT
    EXECUTE FUNCTION collisions_replicator.if_modified_func('true');