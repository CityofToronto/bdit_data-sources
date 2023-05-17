-- Table containing max, min, mean temperature of the day,
-- as well as total rain, snow and precip 
-- of Toronto Pearson Airport, station_id = 51459
-- Updated daily from government of canada's historical weather data
-- run by DAG `pull_weather.py`.

CREATE TABLE IF NOT EXISTS weather.historical_daily_airport
(
    dt date NOT NULL,
    temp_max numeric,
    temp_min numeric,
    mean_temp numeric,
    total_rain numeric,
    total_snow numeric,
    total_precip numeric,
    CONSTRAINT historical_daily_airport_pkey PRIMARY KEY (dt)
)

ALTER TABLE weather.historical_daily_airport
    OWNER to weather_admins;

REVOKE ALL ON TABLE weather.historical_daily_airport FROM weather_bot;

GRANT SELECT ON TABLE weather.historical_daily_airport TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE weather.historical_daily_airport TO weather_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE weather.historical_daily_airport TO weather_bot;

COMMENT ON TABLE weather.historical_daily_airport
    IS 'Contains daily weather data around Toronto Pearson Airport. Data pulled daily from https://climate.weather.gc.ca/. Station_id = 51459. ';