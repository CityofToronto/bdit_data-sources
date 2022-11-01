INSERT INTO weather.prediction_daily
(
    SELECT * from weather.prediction_daily
    EXCEPT
    

)
ON CONFLICT (datetime_bin::date)
DO UPDATE
SET 