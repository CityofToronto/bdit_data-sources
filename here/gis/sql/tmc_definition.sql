DROP TABLE IF EXISTS here_gis.tmc_definition_staging;
CREATE TABLE here_gis.tmc_definition_staging
(
    province TEXT NOT NULL,
    region TEXT NOT NULL,
    tmc char(9) NOT NULL PRIMARY KEY,
    tmc_length_km double precision NOT NULL, 
    parent_linear char(9),
	LINEAR char(9) NOT NULL,
	TMC_ORDER_INDEX smallint NOT NULL,
	ROAD_NUMBER TEXT NULL,
	ROAD_NAME TEXT NULL,
	ROAD_DIRECTION TEXT NOT NULL,
	POINT_DESCRIPTION TEXT NULL,
	TMC_TYPE varchar(10) NOT NULL,
	POSITIVE_OFFSET char(9) NULL,
	NEGATIVE_OFFSET char(9) NULL,
	START_LAT numeric(7,5) NOT NULL,
	START_LNG numeric(7,5) NOT NULL,
	END_LAT numeric(7,5) NULL,
	END_LNG numeric(7,5) NULL,
	CONTROLLED_ACCESS char(1) NOT NULL
);
COMMENT ON TABLE here_gis.tmc_definition_staging IS 'Staging table for tmc definition imports, this is what the CSVs actually look like';

DROP TABLE IF EXISTS here_gis.tmc_definition;
CREATE TABLE here_gis.tmc_definition
(
    tmc char(9) NOT NULL PRIMARY KEY,
    tmc_length_km double precision NOT NULL, 
    parent_linear char(9),
	LINEAR char(9) NOT NULL,
	TMC_ORDER_INDEX smallint NOT NULL,
	ROAD_NUMBER TEXT NULL,
	ROAD_NAME TEXT NULL,
	ROAD_DIRECTION TEXT NOT NULL,
	POINT_DESCRIPTION TEXT NULL,
	TMC_TYPE varchar(10) NOT NULL,
	POSITIVE_OFFSET char(9) NULL,
	NEGATIVE_OFFSET char(9) NULL,
    start_pnt geometry(point,4326) NOT NULL,
	end_pnt geometry(point,4326) NULL,
	CONTROLLED_ACCESS boolean NOT NULL,
    valid_range daterange NOT NULL
);

CREATE OR REPLACE FUNCTION here_gis.xfer_tmc_definition(yr int, quarter int) 
	RETURNS INTEGER 
    LANGUAGE 'plpgsql'
    VOLATILE STRICT
    AS $body$
    /*Author: Raphael Dumas
    Upload a TMC definition to the staging table then run this function with the correct yr and quarter for the TMC definition data
    Example Usage:
    	SELECT here_gis.xfer_tmc_definition(2016, 4);
    */
    
    DECLARE 
    	start_date DATE;
        end_date DATE;
        tablename TEXT := 'tmc_definition'||yr||'Q'||quarter;
    BEGIN
    	
   		IF yr BETWEEN 2000 and 2100 AND quarter BETWEEN 1 AND 4 THEN 
        	start_date := (to_date(yr::TEXT, 'YYYY') + (quarter - 1) * INTERVAL '3 months')::DATE;
            end_date := (to_date(yr::TEXT, 'YYYY') + (quarter) * INTERVAL '3 months')::DATE;
            EXECUTE FORMAT('CREATE TABLE here_gis.%I ()INHERITS (here_gis.tmc_definition)', tablename);
            EXECUTE FORMAT($$INSERT INTO here_gis.%I
                    SELECT tmc, tmc_length_km, parent_linear, linear, tmc_order_index, road_number, road_name, road_direction, point_description, tmc_type, positive_offset, negative_offset, 
                           ST_setSRID(ST_MAKEPOINT(start_lng, start_lat), 4326) AS start_pnt, 
                           ST_setSRID(ST_MAKEPOINT(end_lng,end_lat), 4326) AS end_pnt,
                           CASE controlled_access WHEN 'Y' THEN TRUE WHEN 'N' THEN FALSE END AS controlled_access,
                            daterange(%L, %L) AS valid_range
                    FROM here_gis.tmc_definition_staging;
                    ALTER TABLE here_gis.%I ADD CHECK (valid_range = daterange(%L, %L) )$$, tablename, start_date, end_date, tablename, start_date, end_date);
            TRUNCATE here_gis.tmc_definition_staging;
            RETURN 1;
        ELSE
        	RAISE EXCEPTION 'Invalid year or quarter values';
        END IF;
        
    END;
    $body$;
    ALTER FUNCTION here_gis.xfer_tmc_definition(int, int)  OWNER TO here_gis_admins;
