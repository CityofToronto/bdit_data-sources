INSERT INTO weather.prediction_daily
(
    dt, temp_max, temp_min, precip_prob, text_summary_day, text_summary_night, date_pulled
) VALUES %s
ON CONFLICT (dt)
DO UPDATE
SET (temp_max, temp_min, precip_prob, text_summary_day, text_summary_night, date_pulled)
    = (EXCLUDED.temp_max, EXCLUDED.temp_min, EXCLUDED.precip_prob, EXCLUDED.text_summary_day, EXCLUDED.text_summary_night, EXCLUDED.date_pulled)