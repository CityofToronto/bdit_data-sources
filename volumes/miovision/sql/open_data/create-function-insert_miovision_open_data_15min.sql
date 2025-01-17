--Review decisions:
--Classification Grouping and naming
--Include/Exclude bicycles?
--Include/Exclude buses/streetcars?
--Decision to not include manual anomalous_range 'valid_caveat' notes: SELECT 
--Including entry/exit information to satisfy ATR related DRs.
-->> providing exit leg and direction as extra columns rather
-->> than extra rows to reduce potential for double counting.

--DROP FUNCTION gwolofs.insert_miovision_15min_open_data;

CREATE OR REPLACE FUNCTION gwolofs.insert_miovision_15min_open_data(
    _date date,
    integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$
    
    DECLARE
        target_intersections integer [] = miovision_api.get_intersections_uids(intersections);
        n_deleted int;
        n_inserted int;
        _month date = date_trunc('month', _date);

    BEGIN
    
        WITH deleted AS (
            DELETE FROM gwolofs.miovision_15min_open_data
            WHERE
                datetime_15min >= _month
                AND datetime_15min < _month + interval '1 month'
                AND intersection_uid = ANY(target_intersections)
            RETURNING *
        )

        SELECT COUNT(*) INTO n_deleted
        FROM deleted;

        RAISE NOTICE 'Deleted % rows from gwolofs.miovision_15min_open_data for month %.', n_deleted, _month;
        
        CREATE TEMP TABLE miovision_movement_map_new AS (
            SELECT
                entries.movement_uid,
                entries.leg_old AS leg,
                entries.dir AS entry_dir,
                mov.movement_name AS movement,
                --assign exits for peds, bike entry only movements
                COALESCE(exits.leg_new, entries.leg_old) AS exit_leg,
                COALESCE(exits.dir, entries.dir) AS exit_dir
            FROM miovision_api.movement_map AS entries
            JOIN miovision_api.movements AS mov USING (movement_uid)
            LEFT JOIN miovision_api.movement_map AS exits ON
                exits.leg_old = entries.leg_old
                AND exits.movement_uid = entries.movement_uid
                AND exits.leg_new = substr(exits.dir, 1, 1) --eg. E leg going East is an exit
            WHERE entries.leg_new <> substr(entries.dir, 1, 1) --eg. E leg going West is an entry
        );        

        WITH inserted AS (
            INSERT INTO gwolofs.miovision_15min_open_data (
                intersection_uid, intersection_long_name, datetime_15min, classification_type,
                entry_leg, entry_dir, movement, exit_leg, exit_dir, volume_15min
            )
            SELECT
                v15.intersection_uid,
                i.api_name AS intersection_long_name,
                v15.datetime_bin AS datetime_15min,
                CASE
                    WHEN cl.classification = 'Light' THEN 'Light Auto'
                    WHEN cl.classification IN (
                        'SingleUnitTruck', 'ArticulatedTruck', 'MotorizedVehicle', 'Bus'
                    ) THEN 'Truck/Bus'
                    ELSE cl.classification -- 'Bicycle', 'Pedestrian'
                END AS classification_type,           
                v15.leg AS entry_leg,
                mm.entry_dir,
                mm.movement,
                mm.exit_leg,
                mm.exit_dir,
                --assign exits for peds, bike entry only movements
                SUM(v15.volume) AS volume_15min
                --exclude notes (manual text field)
                --array_agg(ar.notes ORDER BY ar.range_start, ar.uid) FILTER (WHERE ar.uid IS NOT NULL) AS anomalous_range_caveats
            FROM miovision_api.volumes_15min_mvt AS v15
            JOIN miovision_api.classifications AS cl USING (classification_uid)
            JOIN miovision_api.intersections AS i USING (intersection_uid)
            -- TMC to ATR crossover table
            JOIN miovision_movement_map_new AS mm USING (movement_uid, leg)
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
                AND ar.problem_level IN ('do-not-use'::text, 'questionable'::text)
            WHERE
                v15.datetime_bin >= _month
                AND v15.datetime_bin < _month + interval '1 month'
                AND v15.intersection_uid = ANY(target_intersections)
            GROUP BY
                v15.intersection_uid,
                i.api_name,
                v15.datetime_bin,
                classification_type,
                v15.leg,
                mm.entry_dir,
                mm.movement,
                mm.exit_leg,
                mm.exit_dir
            HAVING
                NOT array_agg(ar.problem_level) && ARRAY['do-not-use'::text, 'questionable'::text]
                AND SUM(v15.volume) > 0 --confirm
            ORDER BY
                v15.intersection_uid,
                classification_type,
                v15.datetime_bin,
                v15.leg
            RETURNING *
        )

        SELECT COUNT(*) INTO n_inserted
        FROM inserted;

        RAISE NOTICE 'Inserted % rows into gwolofs.miovision_15min_open_data for month %.', n_inserted, _month;

END;
$BODY$;

ALTER FUNCTION gwolofs.insert_miovision_15min_open_data(date, integer [])
OWNER TO gwolofs;

GRANT EXECUTE ON FUNCTION gwolofs.insert_miovision_15min_open_data(date, integer [])
TO miovision_admins;

GRANT EXECUTE ON FUNCTION gwolofs.insert_miovision_15min_open_data(date, integer [])
TO miovision_api_bot;

REVOKE ALL ON FUNCTION gwolofs.insert_miovision_15min_open_data(date, integer [])
FROM public;

COMMENT ON FUNCTION gwolofs.insert_miovision_15min_open_data(date, integer [])
IS 'Function for first deleting then inserting monthly 15
minute open data volumes into gwolofs.miovision_15min_open_data.
Contains an optional intersection parameter in case one just one
intersection needs to be refreshed.';

--testing, around 50 minutes for 1 month (5M rows)
SELECT gwolofs.insert_miovision_15min_open_data('2024-02-01'::date);