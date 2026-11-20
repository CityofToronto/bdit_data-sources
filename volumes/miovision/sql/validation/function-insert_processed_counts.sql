CREATE OR REPLACE FUNCTION miovision_validation.insert_processed_counts()
RETURNS void AS $$

    WITH to_insert AS (
        SELECT
            studies.intersection_uid,
            studies.count_id,
            studies.count_date,
            dat.time_start AS datetime_bin,
            mmp.spec_class,
            mmp.mio_leg AS leg,
            mmp.mio_movement AS movement_name,
            mmp.spec_movements,
            mmp.classification_uids,
            mmp.movement_uids,
            lat.spectrum_count AS spec_count,
            mv.miovision_api_volume
        FROM miovision_validation.spectrum_studies AS studies
        --get the time bins from tmc study
        JOIN traffic.tmc_study_data AS dat USING (count_id)
        --cross join to get every movement
        CROSS JOIN miovision_validation.spec_class_move_map AS mmp,
        --anti join denylist to exclude invalid (wrong way) movements
        LATERAL (
            SELECT COUNT(imd.*) AS invalid_movements
            FROM miovision_api.intersection_movements_denylist AS imd
            WHERE 
                imd.intersection_uid = studies.intersection_uid
                AND imd.classification_uid = ANY(mmp.classification_uids)
                AND imd.leg = mmp.mio_leg
                AND imd.movement_uid = ANY(mmp.movement_uids)
        ) AS imd,
        --sum the right element of the spectrum study json based on spec_movement
        LATERAL (
            SELECT SUM(NULLIF(to_jsonb(dat.*) ->> unnested.spec_movement, ''::text)::numeric) AS spectrum_count
            FROM UNNEST(mmp.spec_movements) AS unnested(spec_movement)
        ) AS lat (spectrum_count),
        --lateral select Miovision volumes
        LATERAL (
            SELECT COALESCE(SUM(v.volume), 0) AS miovision_api_volume
            FROM miovision_api.volumes AS v
            WHERE
                v.datetime_bin >= dat.time_start
                AND v.datetime_bin < dat.time_end
                AND v.intersection_uid = studies.intersection_uid
                AND v.leg = mmp.mio_leg
                AND v.classification_uid = ANY(mmp.classification_uids)
                AND v.movement_uid = ANY(mmp.movement_uids)
        ) AS mv (miovision_api_volume)
        WHERE
            studies.processed = False
            AND imd.invalid_movements = 0 --movements not in denylist
    ),
    
    --only insert results with both non-zero spectrum and miovision api results.
    grouped AS (
        SELECT
            intersection_uid,
            count_date
        FROM to_insert
        GROUP BY
            intersection_uid,
            count_date
        HAVING
            SUM(spec_count) > 0
            AND SUM(miovision_api_volume) > 0
    ),

    inserted AS (
        INSERT INTO miovision_validation.mio_spec_processed_counts (
            intersection_uid, count_id, count_date, datetime_bin, spec_movements, leg, spec_class,
            classification_uids, movement_name, movement_uids, spec_count, miovision_api_volume
        )
        SELECT
            intersection_uid, count_id, count_date, datetime_bin, spec_movements, leg, spec_class,
            classification_uids, movement_name, movement_uids, spec_count, miovision_api_volume
        FROM to_insert
        JOIN grouped USING (intersection_uid, count_date)
    )

    UPDATE miovision_validation.spectrum_studies
    SET processed = True
    FROM grouped
    WHERE
        spectrum_studies.count_date = grouped.count_date
        AND spectrum_studies.intersection_uid = grouped.intersection_uid;
$$
LANGUAGE sql
SECURITY DEFINER;

ALTER FUNCTION miovision_validation.insert_processed_counts OWNER TO miovision_validators;

GRANT EXECUTE ON FUNCTION miovision_validation.insert_processed_counts TO miovision_validation_bot;

/*
To repopulate:
UPDATE miovision_validation.spectrum_studies SET processed = False;
TRUNCATE miovision_validation.mio_spec_processed_counts;
--Popualtes 500 studies/1M rows in 2min
SELECT miovision_validation.insert_processed_counts()
*/

COMMENT ON FUNCTION miovision_validation.insert_processed_counts
IS 'Inserts any new Spectrum data into `miovision_validation.mio_spec_processed_counts` table.
Only tries to process studies from `spectrum_studies` which are not already marked as processed.
Run daily by `miovision_validation` Airflow DAG.';
