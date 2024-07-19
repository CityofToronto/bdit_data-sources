CREATE OR REPLACE FUNCTION weather.airport_weather_summary(
    dt date
)

RETURNS text
LANGUAGE sql
COST 100
VOLATILE

AS $BODY$
    WITH input_data AS (
        SELECT airport_weather_summary.dt
    )

    SELECT
        'Weather for ' || dt || ': '
        || COALESCE(temp_min || ' to ' || temp_max || 'C, '
        || CASE
            WHEN COALESCE(total_precip,0) = 0 THEN 'No precip.'
            WHEN COALESCE(total_snow,0) = 0 THEN COALESCE(total_rain,0) || 'mm rain. '
            WHEN COALESCE(total_rain,0) = 0 THEN COALESCE(total_snow,0) || 'mm snow. '
            ELSE COALESCE(total_rain,0) || 'mm rain, ' || COALESCE(total_snow,0) || 'mm snow. '
        END, 'Unknown') AS weather_summary
    FROM input_data
    LEFT JOIN weather.historical_daily_airport USING (dt);
    
$BODY$;

ALTER FUNCTION weather.airport_weather_summary(date)
OWNER TO weather_admins;

GRANT EXECUTE ON FUNCTION weather.airport_weather_summary(date) TO bdit_humans;
GRANT EXECUTE ON FUNCTION weather.airport_weather_summary(date) TO bdit_bots;

COMMENT ON FUNCTION weather.airport_weather_summary(date)
IS '''Function to return a human readable weather summary for a date.''';