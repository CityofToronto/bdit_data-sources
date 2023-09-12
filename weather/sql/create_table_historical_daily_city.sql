-- Table containing max, min, mean temperature of the day,
-- as well as total rain, snow and precip 
-- of Toronto City Centre, station_id = 31688
-- Updated daily from government of canada's historical weather data
-- run by DAG `pull_weather.py`.

CREATE TABLE IF NOT EXISTS weather.historical_daily_city
(
    dt date NOT NULL,
    temp_max numeric,
    temp_min numeric,
    mean_temp numeric,
    total_rain numeric,
    total_snow numeric,
    total_precip numeric,
    CONSTRAINT historical_daily_city_pkey PRIMARY KEY (dt)
);

ALTER TABLE weather.historical_daily_city
    OWNER to weather_admins;

REVOKE ALL ON TABLE weather.historical_daily_city FROM weather_bot;

GRANT SELECT ON TABLE weather.historical_daily_city TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE weather.historical_daily_city TO weather_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE weather.historical_daily_city TO weather_bot;

COMMENT ON TABLE weather.historical_daily_city
    IS 'Contains daily weather data for the City of Toronto Centre. Data pulled daily from https://climate.weather.gc.ca/. Station_id = 31688. ';