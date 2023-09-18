--DROP TABLE vds.detector_inventory_cursed;
CREATE TABLE IF NOT EXISTS vds.detector_inventory_cursed
(
    detector_id text COLLATE pg_catalog."default",
    number_of_lanes smallint,
    latitude numeric,
    longitude numeric,
    det_group text COLLATE pg_catalog."default",
    road_class text COLLATE pg_catalog."default",
    primary_road text COLLATE pg_catalog."default",
    direction character varying(1) COLLATE pg_catalog."default",
    offset_distance integer,
    offset_direction text COLLATE pg_catalog."default",
    cross_road text COLLATE pg_catalog."default",
    district text COLLATE pg_catalog."default",
    ward text COLLATE pg_catalog."default",
    vds_type text COLLATE pg_catalog."default",
    total_loops smallint,
    sequence_number text COLLATE pg_catalog."default",
    data_range_low integer,
    data_range_high integer,
    historical_count integer,
    arterycode integer
);

INSERT INTO vds.detector_inventory_cursed
SELECT 
    detector_id,
    number_of_lanes,
    latitude,
    longitude,
    det_group,
    road_class,
    primary_road,
    direction,
    offset_distance,
    offset_direction,
    cross_road,
    district,
    ward,
    vds_type,
    total_loops,
    sequence_number,
    data_range_low,
    data_range_high,
    historical_count,
    arterycode
FROM rescu.detector_inventory;

ALTER TABLE IF EXISTS vds.detector_inventory_cursed OWNER TO vds_admins;
GRANT SELECT ON TABLE vds.detector_inventory_cursed TO bdit_humans;
GRANT ALL ON TABLE vds.detector_inventory_cursed TO vds_admins;

COMMENT ON TABLE vds.detector_inventory_cursed IS 'Store old detector_inventory
table from rescu schema which  contained some manual information of potential
future utility.';
