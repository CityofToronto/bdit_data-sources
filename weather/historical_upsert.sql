INSERT INTO weather.historical_daily
    (
        (dt, temp_max, temp_min, date_pulled)
    ) VALUES %s
    ON CONFLICT (dt)
    DO UPDATE
    SET (temp_max, temp_min, date_pulled)
        = (EXCLUDED.temp_max, EXCLUDED.temp_min, EXCLUDED.date_pulled);

