--Review decisions:
--Classification Grouping and naming
--Include/Exclude bicycles?
--Include/Exclude buses/streetcars?
--Decision to not include manual anomalous_range 'valid_caveat' notes: SELECT 
--Including entry/exit information to satisfy ATR related DRs.
    --> providing exit leg and direction as extra columns rather
    -- than extra rows to reduce potential for double counting.

--DROP VIEW gwolofs.miovision_15min_open_data;

CREATE OR REPLACE VIEW gwolofs.miovision_15min_open_data AS (

    SELECT
        v15.intersection_uid,
        i.api_name AS intersection_long_name,
        v15.datetime_bin AS datetime_15min,
        CASE
            WHEN cl.classification = 'Light' THEN 'Light Auto'
            WHEN cl.classification IN ('SingleUnitTruck', 'ArticulatedTruck', 'MotorizedVehicle', 'Bus') THEN 'Truck/Bus'
            ELSE cl.classification -- 'Bicycle', 'Pedestrian'
        END AS classification_type,                
        v15.leg AS entry_leg,
        entries.dir AS entry_dir,
        mov.movement_name AS movement,
        --assign exits for peds, bike entry only movements
        COALESCE(exits.leg_new, v15.leg) AS exit_leg,
        COALESCE(exits.dir, entries.dir) AS exit_dir,
        SUM(v15.volume) AS volume_15min
        --exclude notes (manual text field)
        --array_agg(ar.notes ORDER BY ar.range_start, ar.uid) FILTER (WHERE ar.uid IS NOT NULL) AS anomalous_range_caveats
    FROM miovision_api.volumes_15min_mvt AS v15
    JOIN miovision_api.intersections AS i USING (intersection_uid)
    JOIN miovision_api.classifications AS cl USING (classification_uid)
    JOIN miovision_api.movements AS mov USING (movement_uid)
    -- TMC to ATR crossover table for e
    LEFT JOIN miovision_api.movement_map AS entries ON 
        entries.leg_old = v15.leg
        AND entries.movement_uid = v15.movement_uid
        AND entries.leg_new <> substr(entries.dir, 1, 1) --eg. E leg going West is an entry
    -- TMC to ATR crossover table
    LEFT JOIN miovision_api.movement_map AS exits ON
        exits.leg_old = v15.leg
        AND exits.movement_uid = v15.movement_uid
        AND exits.leg_new = substr(exits.dir, 1, 1) --eg. E leg going East is an exit
    --anti-join anomalous_ranges. See HAVING clause.
    LEFT JOIN miovision_api.anomalous_ranges AS ar ON
        (
            ar.intersection_uid = v15.intersection_uid
            OR ar.intersection_uid IS NULL
        ) AND (
            ar.classification_uid = v15.classification_uid
            OR ar.classification_uid IS NULL
        )
        AND v15.datetime_bin >= ar.range_start
        AND (
            v15.datetime_bin < ar.range_end
            OR ar.range_end IS NULL
        )
    GROUP BY
        v15.intersection_uid,
        i.api_name,
        v15.datetime_bin,
        classification_type,
        movement,
        entry_leg,
        entry_dir,
        exit_dir,
        exit_leg
    HAVING
        NOT array_agg(ar.problem_level) && ARRAY['do-not-use'::text, 'questionable'::text]
        AND SUM(v15.volume) > 0 --confirm
    ORDER BY
        v15.intersection_uid,
        classification_type,
        v15.datetime_bin,
        v15.leg
);

--testing, indexes work
--50s for 1 day, 40 minutes for 1 month (5M rows)
SELECT *
FROM gwolofs.miovision_15min_open_data
WHERE
    datetime_15min >= '2024-01-01'::date
    AND datetime_15min < '2024-01-02'::date;