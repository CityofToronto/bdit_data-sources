CREATE SCHEMA inrix
  AUTHORIZATION rdumas;

 DROP TABLE IF EXISTS inrix.raw_data_import_test_temp;
 DROP TABLE IF EXISTS inrix.raw_data_import_test;
 
CREATE unlogged TABLE inrix.raw_data_import_test_temp
(
  tx timestamp without time zone NOT NULL,
  tmc char(9) NOT NULL,
  speed integer NOt NULL,
  score smallint not null
 )
WITH (
  OIDS=FALSE
);

BEGIN;
SET LOCAL synchronous_commit=off;
SET LOCAL commit_delay = 80000;
-- SELECT set_config('checkpoint_segments', 64, true);
COPY inrix.raw_data_import_test_temp FROM 'C:\Users\rdumas\Documents\fakedata\Ryerson_Toronto_201605.csv' DELIMITER ',' CSV;
END;

SELECT * 
INTO inrix.raw_data_import_test
FROM inrix.raw_data_import_test_temp;

ALTER TABLE inrix.raw_data_import_test
  ADD PRIMARY KEY (tx, tmc);

  SELECT COUNT( DISTINCT tmc)
  FROM inrix.raw_data_import_test 