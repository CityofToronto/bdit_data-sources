WITH city_check AS (
    SELECT
        COALESCE(
            (SELECT
                temp_max IS NULL
                AND temp_min IS NULL
                AND mean_temp IS NULL
                AND total_rain IS NULL
                AND total_snow IS NULL
                AND total_precip IS NULL
            FROM weather.historical_daily_city
            WHERE dt = '{{ ds }}'::date), --noqa: TMP
            TRUE
        ) AS all_nulls
    FROM (VALUES (1)) AS dummy_ --incase no row found, still fail
),

airport_check AS (
    SELECT
        COALESCE(
            (SELECT
                temp_max IS NULL
                AND temp_min IS NULL
                AND mean_temp IS NULL
                AND total_rain IS NULL
                AND total_snow IS NULL
                AND total_precip IS NULL
            FROM weather.historical_daily_airport
            WHERE dt = '{{ ds }}'::date), --noqa: TMP
            TRUE
        ) AS all_nulls
    FROM (VALUES (1)) AS dummy_ --incase no row found, still fail
)

SELECT
    (SELECT
        (SELECT NOT(all_nulls) FROM city_check)
        AND (SELECT NOT(all_nulls) FROM airport_check)
    ) AS check_,
    CASE (SELECT all_nulls FROM city_check)
        WHEN TRUE THEN
            '`weather.historical_daily_city` input for `'
            || '{{ ds }}'::date || '` is all nulls.' --noqa: TMP
    END AS city_check,
    CASE (SELECT all_nulls FROM airport_check)
        WHEN TRUE THEN
            '`weather.historical_daily_airport` input for `'
            || '{{ ds }}'::date || '` is all nulls.' --noqa: TMP
    END AS airport_check;