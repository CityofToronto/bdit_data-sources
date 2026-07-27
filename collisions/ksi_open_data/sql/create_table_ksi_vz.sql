-- Table: open_data.ksi_vz

-- DROP TABLE IF EXISTS open_data.ksi_vz;

CREATE TABLE IF NOT EXISTS open_data.ksi_vz
(
    "REC_ID" bigint,
    "ACCIDENT_NUMBER" character varying(10),
    "ACCIDENT_YEAR" integer,
    "LONGITUDE" double precision,
    "INJURY" integer,
    "LIGHT_CONDITION" character varying(2),
    "VISIBILITY_CONDITION" character varying(2),
    "STREET1" text,
    "STREET2" text,
    "ACCIDENT_TIME" time without time zone,
    "ROAD_SURFACE_CONDITION" character varying(2),
    "WARD_NUMBER" character varying(40),
    "ACCIDENT_DATE" date,
    "INVOLVEMENT_TYPE" character varying(2),
    "LATITUDE" double precision,
    "INVAGE" text,
    "DATETIME" timestamp without time zone
);

ALTER TABLE IF EXISTS open_data.ksi_vz
OWNER TO od_admins;

GRANT SELECT ON TABLE open_data.ksi_vz TO bdit_humans;

GRANT ALL ON TABLE open_data.ksi_vz TO collisions_bot;

GRANT ALL ON TABLE open_data.ksi_vz TO od_admins;

GRANT SELECT ON TABLE open_data.ksi_vz TO od_extract_svc;

COMMENT ON TABLE open_data.ksi_vz
IS 'Table for KSI VZ Dashboard, hosted on GCC.';