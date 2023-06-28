--DROP TABLE gwolofs.vds_entity_locations; 

CREATE TABLE gwolofs.vds_entity_locations (
    divisionid smallint,
    entitytype smallint,
    entityid integer,
    locationtimestamputc timestamp without time zone,
    latitude double precision,
    longitude double precision,
    altitudemetersasl double precision,
    headingdegrees double precision,
    speedkmh double precision,
    numsatellites integer,
    dilutionofprecision double precision,
    mainroadid integer,
    crossroadid integer,
    secondcrossroadid integer,
    mainroadname character varying,
    crossroadname character varying,
    secondcrossroadname character varying,
    streetnumber character varying,
    offsetdistancemeters double precision,
    offsetdirectiondegrees double precision,
    locationsource smallint,
    locationdescriptionoverwrite character varying,
    PRIMARY KEY(divisionid, entityid, locationtimestamputc)
);

--ALTER TABLE rescu.detector_inventory OWNER TO rescu_admins;