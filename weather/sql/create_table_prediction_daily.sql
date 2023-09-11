-- Table containing forecased max and min temperature of the day 
-- as well as precipitation probability for day and night
-- with text summary
-- Updated daily from Environment Canada's forecast weather data
-- run by DAG `pull_weather.py`.

CREATE TABLE IF NOT EXISTS weather.prediction_daily
(
    dt date NOT NULL,
    temp_max numeric,
    temp_min numeric,
    precip_prob_day numeric,
    precip_prob_night numeric,
    text_summary_day text COLLATE pg_catalog."default",
    text_summary_night text COLLATE pg_catalog."default",
    date_pulled date,
    CONSTRAINT prediction_daily_pkey PRIMARY KEY (dt)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE weather.prediction_daily
    OWNER to weather_admins;

REVOKE ALL ON TABLE weather.prediction_daily FROM weather_bot;

GRANT SELECT ON TABLE weather.prediction_daily TO bdit_humans;

GRANT ALL ON TABLE weather.prediction_daily TO weather_admins;

GRANT DELETE, INSERT, SELECT, UPDATE ON TABLE weather.prediction_daily TO weather_bot;

COMMENT ON TABLE weather.prediction_daily
    IS 'Contains daily prediction weather data from Environment Canada. Data pulled daily from https://climate.weather.gc.ca/.';