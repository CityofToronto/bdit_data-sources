--DROP FUNCTION gwolofs.insert_miovision_open_data_monthly_summary;

CREATE OR REPLACE FUNCTION gwolofs.insert_miovision_open_data_monthly_summary(
    _date date,
    intersections integer [] DEFAULT ARRAY[]::integer []
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
            DELETE FROM gwolofs.miovision_open_data_monthly_summary
            WHERE
                mnth = _month
                AND intersection_uid = ANY(target_intersections)
            RETURNING *
        )

        SELECT COUNT(*) INTO n_deleted
        FROM deleted;

        RAISE NOTICE 'Deleted % rows from gwolofs.miovision_15min_open_data for month %.', n_deleted, _month;
        
    WITH daily_volumes AS (
        SELECT
            vd.dt,
            vd.intersection_uid,
            CASE
                WHEN cl.classification = 'Light' THEN 'Light Auto'
                WHEN cl.classification IN ('SingleUnitTruck', 'ArticulatedTruck', 'MotorizedVehicle', 'Bus') THEN 'Truck/Bus'
                ELSE cl.classification -- 'Bicycle', 'Pedestrian'
            END AS classification_type,
            --daily volume with long gaps imputted
            SUM(coalesce(daily_volume,0) + coalesce(avg_historical_gap_vol,0)) AS total_vol
        --omits anomalous_ranges
        FROM miovision_api.volumes_daily AS vd
        JOIN miovision_api.classifications AS cl USING (classification_uid)
        WHERE
            vd.isodow <= 5
            AND vd.holiday IS false
            AND vd.dt >= _month
            AND vd.dt < _month + interval '1 month'
            AND vd.intersection_uid = ANY(target_intersections)
            --AND classification_uid NOT IN (2,7,10) --exclude bikes due to reliability?
        GROUP BY
            vd.dt,
            vd.intersection_uid,
            vd.intersection_uid,
            classification_type
    ),

    v15 AS (
        --15 minute volumes grouped by classification_type
        SELECT
            v.intersection_uid,
            v.datetime_bin,
            CASE
                WHEN cl.classification = 'Light' THEN 'Light Auto'
                WHEN cl.classification IN ('SingleUnitTruck', 'ArticulatedTruck', 'MotorizedVehicle', 'Bus') THEN 'Truck/Bus'
                ELSE cl.classification -- 'Bicycle', 'Pedestrian'
            END AS classification_type,
            SUM(v.volume) AS vol_15min
        FROM miovision_api.volumes_15min_mvt AS v
        JOIN miovision_api.classifications AS cl USING (classification_uid)
        --anti join holidays
        LEFT JOIN ref.holiday AS hol ON hol.dt = v.datetime_bin::date
        --anti join anomalous ranges. See HAVING clause. 
        --NOTE: this method is omitting the whole classification_type if
            --one classification is missing. May be undesired.
        LEFT JOIN miovision_api.anomalous_ranges ar ON
        (
            ar.intersection_uid = v.intersection_uid
            OR ar.intersection_uid IS NULL
        ) AND (
            ar.classification_uid = v.classification_uid
            OR ar.classification_uid IS NULL
        )
        AND v.datetime_bin >= ar.range_start
        AND (
            v.datetime_bin < ar.range_end
            OR ar.range_end IS NULL
        )
        WHERE
            v.datetime_bin >= _month
            AND v.datetime_bin < _month + interval '1 month'
            AND v.intersection_uid = ANY(target_intersections)
            AND hol.holiday IS NULL
            AND date_part('isodow', v.datetime_bin) <= 5 --weekday
            --AND classification_uid NOT IN (2,7,10) --exclude bikes due to reliability?
        GROUP BY
            v.intersection_uid,
            classification_type,
            v.datetime_bin
    ),

    hourly_data AS (
        --find rolling 1 hour volume
        SELECT
            intersection_uid,
            classification_type,
            datetime_bin,
            datetime_bin::date AS dt,
            CASE WHEN date_part('hour', datetime_bin) < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
            vol_15min,
            SUM(vol_15min) OVER (
                PARTITION BY intersection_uid, classification_type
                ORDER BY datetime_bin
                RANGE BETWEEN '45 minutes' PRECEDING AND CURRENT ROW
            ) AS hr_vol
        FROM v15
    ),

    highest_daily_volume AS (
        --find highest volume each day
        SELECT
            intersection_uid,
            classification_type,
            am_pm,
            dt,
            MAX(hr_vol) AS max_hr_volume
        FROM hourly_data
        GROUP BY
            intersection_uid,
            classification_type,
            am_pm,
            dt
    ),

    inserted AS (
        INSERT INTO gwolofs.miovision_open_data_monthly_summary (
             intersection_uid, intersection_long_name, mnth, avg_daily_vol_auto, avg_daily_vol_truckbus, avg_daily_vol_ped, avg_daily_vol_bike,
             avg_am_peak_hour_vol_auto, avg_am_peak_hour_vol_truckbus, avg_am_peak_hour_vol_ped, avg_am_peak_hour_vol_bike,
             avg_pm_peak_hour_vol_auto, avg_pm_peak_hour_vol_truckbus, avg_pm_peak_hour_vol_ped, avg_pm_peak_hour_vol_bike
        )
        SELECT
            coalesce(dv.intersection_uid, hv.intersection_uid) AS intersection_uid,
            i.api_name AS intersection_long_name,
            date_trunc('month', coalesce(dv.dt, hv.dt)) AS mnth,
            ROUND(AVG(dv.total_vol) FILTER (WHERE dv.classification_type = 'Light Auto'), 0) AS avg_daily_vol_auto,
            ROUND(AVG(dv.total_vol) FILTER (WHERE dv.classification_type = 'Truck/Bus'), 0) AS avg_daily_vol_truckbus,
            ROUND(AVG(dv.total_vol) FILTER (WHERE dv.classification_type = 'Pedestrians'), 0) AS avg_daily_vol_ped,
            ROUND(AVG(dv.total_vol) FILTER (WHERE dv.classification_type = 'Cyclists'), 0) AS avg_daily_vol_bike,
            ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.classification_type = 'Light Auto' AND hv.am_pm = 'AM'), 0) AS avg_am_peak_hour_vol_auto,
            ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.classification_type = 'Truck/Bus' AND hv.am_pm = 'AM'), 0) AS avg_am_peak_hour_vol_truckbus,
            ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.classification_type = 'Pedestrians' AND hv.am_pm = 'AM'), 0) AS avg_am_peak_hour_vol_ped,
            ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.classification_type = 'Cyclists' AND hv.am_pm = 'AM'), 0) AS avg_am_peak_hour_vol_bike,
            ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.classification_type = 'Light Auto' AND hv.am_pm = 'PM'), 0) AS avg_pm_peak_hour_vol_auto,
            ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.classification_type = 'Truck/Bus' AND hv.am_pm = 'PM'), 0) AS avg_pm_peak_hour_vol_truckbus,
            ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.classification_type = 'Pedestrians' AND hv.am_pm = 'PM'), 0) AS avg_pm_peak_hour_vol_ped,
            ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.classification_type = 'Cyclists' AND hv.am_pm = 'PM'), 0) AS avg_pm_peak_hour_vol_bike 
            --array_agg(DISTINCT ar.notes) FILTER (WHERE ar.uid IS NOT NULL) AS notes
        FROM daily_volumes AS dv
        FULL JOIN highest_daily_volume AS hv USING (intersection_uid, dt, classification_type)
        LEFT JOIN miovision_api.intersections AS i ON
            i.intersection_uid = coalesce(dv.intersection_uid, hv.intersection_uid)
        GROUP BY
            coalesce(dv.intersection_uid, hv.intersection_uid),
            i.api_name,
            date_trunc('month', coalesce(dv.dt, hv.dt))
        ORDER BY
            coalesce(dv.intersection_uid, hv.intersection_uid),
            date_trunc('month', coalesce(dv.dt, hv.dt))
        RETURNING *
    )
        
    SELECT COUNT(*) INTO n_inserted
    FROM inserted;

    RAISE NOTICE 'Inserted % rows into gwolofs.miovision_open_data_monthly_summary for month %.', n_inserted, _month;

END;
$BODY$;

ALTER FUNCTION gwolofs.insert_miovision_open_data_monthly_summary(date, integer [])
OWNER TO gwolofs;

GRANT EXECUTE ON FUNCTION gwolofs.insert_miovision_open_data_monthly_summary(date, integer [])
TO miovision_admins;

GRANT EXECUTE ON FUNCTION gwolofs.insert_miovision_open_data_monthly_summary(date, integer [])
TO miovision_api_bot;

REVOKE ALL ON FUNCTION gwolofs.insert_miovision_open_data_monthly_summary(date, integer [])
FROM public;

COMMENT ON FUNCTION gwolofs.insert_miovision_open_data_monthly_summary(date, integer [])
IS 'Function for first deleting then inserting monthly summary miovision
open data into gwolofs.miovision_open_data_monthly_summary.
Contains an optional intersection parameter in case one just one
intersection needs to be refreshed.';

--testing, indexes work
--~50s for 1 day, ~40 minutes for 1 month (5M rows)
SELECT gwolofs.insert_miovision_open_data_monthly_summary('2024-02-01'::date);