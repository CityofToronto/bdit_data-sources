CREATE OR REPLACE FUNCTION miovision_validation.insert_processed_counts()
RETURNS void AS $$
    WITH inserted AS (
        INSERT INTO miovision_validation.mio_spec_processed_counts (
            intersection_uid, count_id, count_date, datetime_bin, spec_movements, leg, spec_class,
            classification_uids, movement_name, movement_uids, spec_count, miovision_api_volume
        )
        SELECT
            studies.intersection_uid,
            studies.count_id,
            studies.count_date,
            dat.time_start AS datetime_bin,
            ARRAY_AGG(mmp.spec_movement ORDER BY mmp.spec_movement) AS spec_movements,
            mmp.mio_leg AS leg,
            mmp.spec_class,
            ARRAY_AGG(unnested.mio_class ORDER BY unnested.mio_class) AS classification_uids,
            mmp.mio_movement AS movement_name,
            mmp.mio_move_uid AS movement_uids,
            SUM(lat.spectrum_count) AS spec_count,
            COALESCE(SUM(v15.volume), 0) AS miovision_api_volume
        FROM miovision_validation.spectrum_studies AS studies
        --get the time bins from tmc study
        JOIN traffic.tmc_study_data AS dat USING (count_id)
        --cross join to get every movement
        CROSS JOIN miovision_validation.spec_class_move_map_agg AS mmp
        LEFT JOIN miovision_api.volumes_15min_mvt_unfiltered AS v15
            ON v15.datetime_bin = dat.time_start
            AND v15.intersection_uid = studies.intersection_uid
            AND v15.leg = mmp.mio_leg
            AND v15.classification_uid = ANY(mmp.mio_class_uid)
            AND v15.movement_uid = ANY(mmp.mio_move_uid),
        LATERAL (
            SELECT NULLIF(to_jsonb(dat.*) ->> mmp.spec_movement, ''::text)::numeric AS spectrum_count
        ) AS lat(spectrum_count),
        UNNEST(mmp.mio_class_uid) AS unnested(mio_class)
        WHERE studies.processed = False
        GROUP BY
            studies.intersection_uid,
            studies.count_id,
            studies.count_date,
            dat.time_start,
            mmp.mio_leg,
            mmp.spec_class,
            mmp.mio_movement,
            mmp.mio_move_uid
        RETURNING intersection_uid, count_date
    )

    UPDATE miovision_validation.spectrum_studies
    SET processed = True
    FROM inserted
    WHERE
        spectrum_studies.count_date = inserted.count_date
        AND spectrum_studies.intersection_uid = inserted.intersection_uid;
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
