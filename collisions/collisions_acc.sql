-- Table schema for storing data from /data/replicator/flashcrow-CRASH/dat/ACC.dat.

CREATE TABLE collisions.acc (
      "ACCNB" varchar(10) NOT NULL
    , "RELACCNB" varchar(10)
    , "ACCDATE" timestamp
    , "DAY_NO" varchar(1)
    , "ACCTIME" varchar(4)
    , "PATAREA" varchar(4)
    , "STNAME1" varchar(35)
    , "STREETYPE1" varchar(4)
    , "DIR1" varchar(1)
    , "STNAME2" varchar(35)
    , "STREETYPE2" varchar(4)
    , "DIR2" varchar(1)
    , "STNAME3" varchar(35)
    , "STREETYPE3" varchar(4)
    , "DIR3" varchar(1)
    , "PER_INV" varchar(2)
    , "VEH_INV" varchar(2)
    , "MUNICIPAL" varchar(2)
    , "LOCCOORD" varchar(2)
    , "IMPCTAREA" varchar(2)
    , "ACCLASS" varchar(2)
    , "ACCLOC" varchar(2)
    , "TRAFFICTL" varchar(2)
    , "DRIVAGE" varchar(2)
    , "VEH_NO" varchar(2)
    , "VEHTYPE" varchar(2)
    , "TOWEDVEH" varchar(2)
    , "INITDIR" varchar(2)
    , "IMPACTYPE" varchar(2)
    , "IMPLOC" varchar(2)
    , "EVENT1" varchar(2)
    , "EVENT2" varchar(2)
    , "EVENT3" varchar(2)
    , "PER_NO" varchar(2)
    , "INVTYPE" varchar(2)
    , "INVAGE" varchar(2)
    , "INJURY" varchar(1)
    , "SAFEQUIP" varchar(2)
    , "DRIVACT" varchar(2)
    , "DRIVCOND" varchar(2)
    , "PEDCOND" varchar(2)
    , "PEDACT" varchar(2)
    , "CHARGE" varchar(5)
    , "CHARGE2" varchar(5)
    , "CHARGE3" varchar(5)
    , "CHARGE4" varchar(5)
    , "VISIBLE" varchar(2)
    , "LIGHT" varchar(2)
    , "RDSFCOND" varchar(2)
    , "VEHIMPTYPE" varchar(2)
    , "MANOEUVER" varchar(2)
    , "ENTRY" varchar(1)
    , "GEOCODE" varchar(6)
    , "FACTOR_ERR" varchar(1)
    , "REP_TYPE" varchar(1)
    , "BADGE_NO" varchar(8)
    , "POSTAL" varchar(7)
    , "XCOORD" varchar(10)
    , "YCOORD" varchar(10)
    , "PRECISE_XY" varchar(1)
    , "LONGITUDE" float8
    , "LATITUDE" float8
    , "CHANGED" int2
    , "SENT_UNIT" varchar(12)
    , "SENT_DATE" timestamp
    , "STATUS" varchar(12)
    , "CITY_AREA" varchar(12)
    , "USER_ID" varchar(12)
    , "CRC_UNIT" varchar(12)
    , "COMMENTS" varchar(5000)
    , "MTP_DIVISION" varchar(12)
    , "POLICE_AGENCY" varchar(12)
    , "SUBMIT_BADGE_NUMBER" varchar(10)
    , "SUBMIT_DATE" timestamp
    , "BIRTHDATE" timestamp
    , "PRIVATE_PROPERTY" varchar(1)
    , "PERSON_ID" int8
    , "USERID" varchar(50)
    , "TS" timestamp
    , "ROAD_CLASS" varchar(50)
    , "SYMBOL_NUM" int8
    , "ROTATION_NUM" int8
    , "PX" varchar(10)
    , "DISTRICT" varchar(30)
    , "QUADRANT" varchar(5)
    , "FAILTOREM" int2
    , "YEAR" varchar(5)
    , "REC_ID" int8 NOT NULL
    , "PEDTYPE" varchar(2)
    , "CYCLISTYPE" varchar(2)
    , "SIDEWALKCYCLE" varchar(10)
    , "CYCACT" varchar(2)
    , "CYCCOND" varchar(2)
    , "MVAIMG" int2
    , "WARDNUM" varchar(40)
    , "FATAL_NO" float8
    , "DESCRIPTION" varchar(4000)
    , "TAB_REPORT" varchar(500)
    , "ACTUAL_SPEED" int2
    , "POSTED_SPEED" int2
    , "TRAFCTLCOND" varchar(2)
    , PRIMARY KEY ("REC_ID")
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE collisions.acc
    OWNER to collision_admins;

GRANT SELECT ON TABLE collisions.acc TO bdit_humans;
GRANT SELECT ON TABLE collisions.acc TO rsaunders;
GRANT SELECT ON TABLE collisions.acc TO kchan;

COMMENT ON TABLE collisions.acc
    IS 'Raw collision database, from Flashcrow /data/replicator/flashcrow-CRASH/dat/ACC.dat.'

CREATE INDEX collisions_acc_idx
    ON collisions.acc USING btree
    ("ACCNB", "ACCDATE")
    TABLESPACE pg_default;
