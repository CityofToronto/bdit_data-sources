DROP TABLE IF EXISTS cycling.trip_surveys;
CREATE TABLE cycling.trip_surveys (
	trip_id bigint PRIMARY KEY,
	app_user_id int NOT NULL,
	started_at timestamp NOT NULL,
	purpose_id smallint NOT NULL,
	notes TEXT
	);

DROP TABLE IF EXISTS cycling.user_surveys;
CREATE TABLE cycling.user_surveys (
	app_user_id int PRIMARY KEY,
	winter smallint NOT NULL,
	rider_history smallint NOT NULL,
	work_zip varchar(7), --Postal Code
	income smallint NOT NULL,
	cycling_req smallint ,
	age  smallint NOT NULL,
	cycling_level smallint NOT NULL,
	gender smallint NOT NULL,
	rider_type smallint,
	school_zip varchar(7),
	home_zip varchar(7) ,
	cycling_exp smallint not null,
	preference_key_userpref boolean
);

CREATE TABLE cycling.trips (
	coord_id  bigint  NOT NULL,
	trip_id  bigint   NOT NULL,
	recorded_at  timestamp   NOT NULL,
	longitude  TEXT   NOT NULL,
	latitude  TEXT   NOT NULL,
	altitude  float NOT NULL,
	speed  numeric   NOT NULL,
	hort_accuracy  float  NOT NULL,
	vert_accuracy  float NOT NULL
);

CREATE TABLE cycling.survey_keys(
	data_ JSON 
);

CREATE TABLE cycling.trip_purpose(
	purpose_id smallserial primary key,
	purpose TEXT UNIQUE
);