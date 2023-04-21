DROP TABLE IF EXISTS cycling.trip_surveys;
CREATE TABLE cycling.trip_surveys (
    trip_id bigint PRIMARY KEY,
    app_user_id int NOT NULL,
    started_at timestamp NOT NULL,
    purpose_id smallint NOT NULL,
    notes text
);

DROP TABLE cycling.user_surveys;
CREATE TABLE cycling.user_surveys
(
    app_user_id integer NOT NULL,
    winter smallint,
    rider_history smallint,
    work_zip text,
    income smallint,
    cycling_req smallint,
    age smallint,
    cycling_level smallint,
    gender smallint,
    rider_type smallint,
    school_zip text,
    home_zip text,
    cycling_exp smallint,
    preference_key_userpref boolean,
    CONSTRAINT user_surveys_pkey PRIMARY KEY (app_user_id)
);

CREATE TABLE cycling.trips (
    coord_id bigint NOT NULL,
    trip_id bigint NOT NULL,
    recorded_at timestamp NOT NULL,
    longitude text NOT NULL,
    latitude text NOT NULL,
    altitude float NOT NULL,
    speed numeric NOT NULL,
    hort_accuracy float NOT NULL,
    vert_accuracy float NOT NULL
);

CREATE TABLE cycling.survey_keys (
    data_ json
);

CREATE TABLE cycling.trip_purpose (
    purpose_id smallserial PRIMARY KEY,
    purpose text UNIQUE
);