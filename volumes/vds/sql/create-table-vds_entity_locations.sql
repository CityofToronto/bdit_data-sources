--DROP TABLE vds.entity_locations; 

CREATE TABLE vds.entity_locations (
    divisionid smallint,
    entitytype smallint,
    entityid integer,
    locationtimestamp timestamp without time zone,
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

ALTER TABLE vds.entity_locations OWNER TO vds_admins;
GRANT INSERT ON TABLE vds.entity_locations TO vds_bot;