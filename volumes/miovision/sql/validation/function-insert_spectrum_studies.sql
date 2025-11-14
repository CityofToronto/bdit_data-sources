CREATE OR REPLACE FUNCTION miovision_validation.insert_spectrum_studies()
RETURNS void AS $$
    INSERT INTO miovision_validation.spectrum_studies (
        intersection_uid, int_id, count_date, count_id
    )
    SELECT
        mio.intersection_uid,
        tmc.intersection_id AS int_id,
        tmc.count_date,
        tmc.count_id
    FROM traffic.tmc_metadata AS tmc
    LEFT JOIN miovision_api.intersections AS mio ON tmc.intersection_id = mio.int_id
    --anti join already inserted studies
    LEFT JOIN miovision_validation.spectrum_studies AS dupes USING (intersection_uid, count_date)
    WHERE
        tmc.count_date > mio.date_installed
        AND (
            tmc.count_date < mio.date_decommissioned
            OR mio.date_decommissioned IS NULL
        )
        --AND tmc.count_date > '2025-01-01'::date
        AND tmc.count_type = 'TMC'::text
        AND dupes.count_id IS NULL;
$$
LANGUAGE sql
SECURITY DEFINER;

ALTER FUNCTION miovision_validation.insert_spectrum_studies OWNER TO miovision_validators;

GRANT EXECUTE ON FUNCTION miovision_validation.insert_spectrum_studies TO miovision_validation_bot;
