CREATE OR REPLACE FUNCTION miovision_validation.insert_spectrum_studies()
RETURNS void AS $$

    --1. Insert any new studies, and "unprocess" modified studies (identified using veh_appr volumes).
    WITH inserted AS (
        INSERT INTO miovision_validation.spectrum_studies (
            intersection_uid, int_id, count_date, count_id,
            count_veh_n_appr, count_veh_s_appr, count_veh_e_appr, count_veh_w_appr
        )
        SELECT
            mio.intersection_uid,
            tmc.intersection_id AS int_id,
            tmc.count_date,
            tmc.count_id,
            tss.count_veh_n_appr,
            tss.count_veh_s_appr,
            tss.count_veh_e_appr,
            tss.count_veh_w_appr
        FROM traffic.tmc_metadata AS tmc
        LEFT JOIN traffic.tmc_summary_stats AS tss USING (count_id)
        LEFT JOIN miovision_api.intersections AS mio ON tmc.intersection_id = mio.int_id
        --anti join already inserted studies
        LEFT JOIN miovision_validation.spectrum_studies AS dupes USING (
            intersection_uid, count_date,
            count_veh_n_appr, count_veh_s_appr, count_veh_e_appr, count_veh_w_appr
        )
        WHERE
            tmc.count_date > mio.date_installed
            AND (
                tmc.count_date < mio.date_decommissioned
                OR mio.date_decommissioned IS NULL
            )
            --AND tmc.count_date > '2025-01-01'::date
            AND tmc.count_type = 'TMC'::text
            AND dupes.count_id IS NULL
        --if any of the details have changed, mark the study as not processed
        ON CONFLICT ON CONSTRAINT miovision_validation_spectrum_studies_pkey
        DO UPDATE
        SET
            int_id = EXCLUDED.int_id,
            count_veh_n_appr = EXCLUDED.count_veh_n_appr,
            count_veh_s_appr = EXCLUDED.count_veh_s_appr,
            count_veh_e_appr = EXCLUDED.count_veh_e_appr,
            count_veh_w_appr = EXCLUDED.count_veh_w_appr,
            processed = False
        --returns any new or updated rows
        RETURNING intersection_uid, count_id, count_date
    )
    
    DELETE FROM miovision_validation.mio_spec_processed_counts AS mspc
    USING inserted
    WHERE
        mspc.intersection_uid = inserted.intersection_uid
        AND mspc.count_id = inserted.count_id
        AND mspc.count_date = inserted.count_date;
    
    --2. Identify and delete any studies which no longer exist in traffic.tmc_metadata
    WITH to_delete AS (
        SELECT
            intersection_uid,
            int_id,
            count_date,
            count_id
        FROM
            miovision_validation.spectrum_studies
        EXCEPT
        SELECT
            mio.intersection_uid,
            tmc.intersection_id AS int_id,
            tmc.count_date,
            tmc.count_id
        FROM traffic.tmc_metadata AS tmc
        LEFT JOIN miovision_api.intersections AS mio ON tmc.intersection_id = mio.int_id
        WHERE
            tmc.count_date > mio.date_installed
            AND (
                tmc.count_date < mio.date_decommissioned
                OR mio.date_decommissioned IS NULL
            )
            AND tmc.count_type = 'TMC'::text
    )

    --mio_spec_processed_counts also removed using foreign key
    DELETE FROM miovision_validation.spectrum_studies AS ss
    USING to_delete AS del
    WHERE
        ss.intersection_uid = del.intersection_uid
        AND ss.int_id = del.int_id
        AND ss.count_date = del.count_date
        AND ss.count_id = del.count_id;
$$
LANGUAGE sql
SECURITY DEFINER;

ALTER FUNCTION miovision_validation.insert_spectrum_studies OWNER TO miovision_validators;

GRANT EXECUTE ON FUNCTION miovision_validation.insert_spectrum_studies TO miovision_validation_bot;

COMMENT ON FUNCTION miovision_validation.insert_spectrum_studies
IS 'Inserts any new Spectrum studies into `miovision_validation.spectrum_studies` table.
Run daily by `miovision_validation` Airflow DAG.';
