--Would prefer to omit this, unless we *really* want it for
--comparison with existing short term TMC open data.

--include u-turns?
--bikes
--motorized vehicles/streetcars?

CREATE OR REPLACE VIEW gwolofs.miovision_open_data_wide_15min AS (

    SELECT
        v.intersection_uid,
        v.datetime_bin,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'S'
        ), 0) AS sb_car_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'S'
        ), 0) AS sb_car_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'S'
        ), 0) AS sb_car_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'N'
        ), 0) AS nb_car_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'N'
        ), 0) AS nb_car_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'N'
        ), 0) AS nb_car_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'W'
        ), 0) AS wb_car_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'W'
        ), 0) AS wb_car_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'W'
        ), 0) AS wb_car_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'E'
        ), 0) AS eb_car_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'E'
        ), 0) AS eb_car_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[1]::int []) AND v.leg = 'E'
        ), 0) AS eb_car_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'S'
        ), 0) AS sb_truck_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'S'
        ), 0) AS sb_truck_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'S'
        ), 0) AS sb_truck_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'N'
        ), 0) AS nb_truck_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'N'
        ), 0) AS nb_truck_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'N'
        ), 0) AS nb_truck_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'W'
        ), 0) AS wb_truck_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'W'
        ), 0) AS wb_truck_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'W'
        ), 0) AS wb_truck_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'E'
        ), 0) AS eb_truck_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'E'
        ), 0) AS eb_truck_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[4, 5, 9]::int []) AND v.leg = 'E'
        ), 0) AS eb_truck_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'S'
        ), 0) AS sb_bus_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'S'
        ), 0) AS sb_bus_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'S'
        ), 0) AS sb_bus_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'N'
        ), 0) AS nb_bus_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'N'
        ), 0) AS nb_bus_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'N'
        ), 0) AS nb_bus_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'W'
        ), 0) AS wb_bus_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'W'
        ), 0) AS wb_bus_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'W'
        ), 0) AS wb_bus_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 3 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'E'
        ), 0) AS eb_bus_r,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 1 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'E'
        ), 0) AS eb_bus_t,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.movement_uid = 2 AND v.classification_uid = ANY(ARRAY[3]::int []) AND v.leg = 'E'
        ), 0) AS eb_bus_l,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.classification_uid = 6
            AND v.leg = 'N'
        ), 0) AS nx_peds,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.classification_uid = 6
            AND v.leg = 'S'
        ), 0) AS sx_peds,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.classification_uid = 6
            AND v.leg = 'E'
        ), 0) AS ex_peds,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.classification_uid = 6
            AND v.leg = 'W'
        ), 0) AS wx_peds,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.classification_uid = 2
            AND v.leg = 'N'
        ), 0) AS nx_bike,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.classification_uid = 2
            AND v.leg = 'S'
        ), 0) AS sx_bike,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.classification_uid = 2
            AND v.leg = 'E'
        ), 0) AS ex_bike,
        COALESCE(SUM(v.volume) FILTER (WHERE
            v.classification_uid = 2
            AND v.leg = 'W'
        ), 0) AS wx_bike
    FROM miovision_api.volumes_15min_mvt AS v
    --JOIN miovision_api.classifications AS c USING (classification_uid)
    --JOIN miovision_api.movements AS m USING (movement_uid)
    WHERE
        v.datetime_bin >= '2024-02-01'::date
        AND v.datetime_bin < '2024-03-01'::date
    --AND intersection_uid = 1
    --AND v.classification_uid NOT IN (2,10) --exclude bikes due to reliability
    GROUP BY
        v.intersection_uid,
        v.datetime_bin
    ORDER BY
        v.intersection_uid,
        v.datetime_bin
);

SELECT * FROM gwolofs.miovision_open_data_wide_15min LIMIT 10000; --noqa: L044

/* query used for query development!

SELECT col_name FROM (
SELECT
    'COALESCE(SUM(v.volume) FILTER (WHERE
        v.movement_uid = ' || movement_uid::text ||
    ' AND v.classification_uid = ANY(
        ARRAY[' || string_agg(classification_uid::text, ',') || ']::int [])' ||
    ' AND v.leg = ''' || leg || '''
), 0) AS ' ||
    dir || '_' ||
    CASE classification WHEN 'Light' THEN 'car' WHEN 'Bus' THEN 'bus'
    WHEN 'SingleUnitTruck' THEN 'truck' WHEN 'ArticulatedTruck' THEN 'truck'
    WHEN 'MotorizedVehicle' THEN 'truck' END || '_' ||
    CASE movement_name WHEN 'left' THEN 'l'
    WHEN 'thru' THEN 't' WHEN 'right' THEN 'r' END || ',' AS col_name
FROM miovision_api.movements
CROSS JOIN miovision_api.classifications
CROSS JOIN (VALUES 
    ('sb', 'S', 1), ('nb', 'N', 2), ('wb', 'W', 3), ('eb', 'E', 4)
) AS directions(dir, leg, dir_order)
WHERE
    classification_uid NOT IN (2,6,8,10) --bikes, peds, workvans (dne)
    --AND movement_uid NOT IN (7,8) --entrances, exits
    AND movement_uid IN (1,2,3)
GROUP BY
    CASE classification WHEN 'Light' THEN 1 WHEN 'Bus' THEN 3 WHEN 'SingleUnitTruck' THEN 2
    WHEN 'ArticulatedTruck' THEN 2 WHEN 'MotorizedVehicle' THEN 2 END,
    CASE classification WHEN 'Light' THEN 'car' WHEN 'Bus' THEN 'bus'
    WHEN 'SingleUnitTruck' THEN 'truck' WHEN 'ArticulatedTruck' THEN 'truck'
    WHEN 'MotorizedVehicle' THEN 'truck' END,
    dir_order,
    dir,
    leg,
    movement_uid
ORDER BY
    CASE classification WHEN 'Light' THEN 1 WHEN 'Bus' THEN 3 WHEN 'SingleUnitTruck' THEN 2
    WHEN 'ArticulatedTruck' THEN 2 WHEN 'MotorizedVehicle' THEN 2 END,
    dir_order,
    CASE movement_name WHEN 'left' THEN 3 WHEN 'thru' THEN 2 WHEN 'right' THEN 1 END
) AS vehs

UNION ALL

SELECT col_name FROM (
    SELECT
        'COALESCE(SUM(v.volume) FILTER (WHERE
            v.classification_uid = ' || classification_uid::text ||
        ' AND v.leg = ''' || leg || '''
    ), 0) AS ' ||
        dir || '_' ||
        CASE classification WHEN 'Bicycle' THEN 'bike'
        WHEN 'Pedestrian' THEN 'peds' END || ',' AS col_name
    FROM miovision_api.classifications
    CROSS JOIN (VALUES
        ('sx', 'S', 2), ('nx', 'N', 1), ('wx', 'W', 4), ('ex', 'E', 3)
    ) AS directions(dir, leg, dir_order)
    WHERE
        classification_uid IN (2,6) --bikes, peds
    ORDER BY
        CASE classification WHEN 'Pedestrian' THEN 1 WHEN 'bike' THEN 2 END,
        dir_order
) AS bikes_n_peds
*/