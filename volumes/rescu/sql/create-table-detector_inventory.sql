CREATE TABLE rescu.detector_inventory (
	detector_id text,
	number_of_lanes smallint,
	latitude numeric,
	longitude numeric,
	det_group text,
	road_class text,
	primary_road text,
	direction varchar(1),
	offset_distance int,
	offset_direction text,
	cross_road text,
	district text,
	ward text,
	vds_type text,
	total_loops smallint,
	sequence_number text,
	data_range_low int,
	data_range_high int,
	historical_count int,
	arterycode int
);

ALTER TABLE rescu.detector_inventory OWNER TO rescu_admins;