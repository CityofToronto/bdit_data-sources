-- View that documents which street versions correspond to 
-- which date range in here.ta table 

CREATE TABLE here.street_valid_range AS 

WITH street_v(street_version, valid_range) AS (
	VALUES 
    ('17_4', '["2012-01-01","2014-01-01")'::daterange),
	('18_3', '["2014-01-01","2017-01-01")'::daterange),
	('19_4_tc', '["2017-01-01","2017-09-01")'::daterange),
	('21_1', '["2017-09-01","2022-08-15")'::daterange),
	('22_2', '["2022-08-15",)'::daterange)
)

SELECT 
	street_version,
	valid_range
FROM street_v;

ALTER TABLE here.street_valid_range OWNER TO here_admins;

COMMENT ON TABLE here.street_valid_range
    IS 'Table that documents which street version here.ta''s data is based on for specific date range. For example, date from 2014-01-01 till 2017-01-01 uses street version 18_3. 
This table gets update every year when we update our here map version. ';