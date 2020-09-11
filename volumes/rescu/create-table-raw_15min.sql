DROP TABLE rescu.raw_15min;

CREATE TABLE rescu.raw_15min (
	raw_uid serial not null,
	dt date,
	raw_info text
	);
	
ALTER TABLE rescu.raw_15min OWNER TO rescu_admins;