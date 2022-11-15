INSERT INTO weather.historical_daily
        (dt, temp_max, temp_min, total_precip_mm, date_pulled)
    VALUES %s
    ON CONFLICT (dt)
    DO UPDATE
    SET (temp_max, temp_min, total_precip_mm, date_pulled)
        = (EXCLUDED.temp_max, EXCLUDED.temp_min, EXCLUDED.total_precip_mm, EXCLUDED.date_pulled);