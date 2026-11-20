DROP FUNCTION gwolofs.test_spec_error_agg_mvmt();
CREATE OR REPLACE FUNCTION gwolofs.test_spec_error_agg_mvmt()
RETURNS TABLE (scenario text, passed boolean, got jsonb, expected jsonb)
LANGUAGE plpgsql AS
$$
DECLARE
    s record;
    r record;
BEGIN
    FOR s IN
        SELECT * FROM miovision_validation.validation_scenarios ORDER BY scenario_name
    LOOP
        DELETE FROM miovision_validation.mio_spec_processed_counts
        WHERE (intersection_uid, count_id) = (-1, -1);
        DELETE FROM miovision_validation.spectrum_studies
        WHERE (intersection_uid, count_id) = (-1, -1);

        INSERT INTO miovision_validation.spectrum_studies (intersection_uid, count_id, count_date)
        VALUES (-1, -1, '2023-01-01');

        INSERT INTO miovision_validation.mio_spec_processed_counts (
            intersection_uid, count_id, count_date, datetime_bin, spec_movements, leg, spec_class,
            movement_name, spec_count, miovision_api_volume, classification_uids, movement_uids
        )
        SELECT
            -1, -1, '2023-01-01',
            '2023-01-01'::date + make_time((b.i - 1)::int, 0, 0),  -- max 24 bins
            s.spec_movements, s.leg, s.spec_class, s.movement_name,
            b.spec, b.mio,
            s.classification_uids, s.movement_uids
        FROM unnest(s.spec_counts, s.mio_counts) WITH ORDINALITY AS b(spec, mio, i);

        SELECT v.* INTO r
        FROM miovision_validation.spec_error_agg_mvmt AS v
        WHERE (v.intersection_uid, v.count_id) = (-1, -1);

        scenario := s.scenario_name;

        expected := jsonb_build_object(
            'leg', s.leg,
            'classification', s.spec_class,
            'movement', s.movement_name,
            'vol_spec', s.exp_vol_spec,
            'vol_mio', s.exp_vol_mio,
            'aggregate_error_with_signs', round(s.exp_err, 6),
            'pass', s.exp_pass,
            'num_bin_used', s.exp_bins
        );

        got := jsonb_build_object(
            'leg', r.leg,
            'classification', r.classification,
            'movement', r.movement,
            'vol_spec', r.vol_spec,
            'vol_mio', r.vol_mio,
            'aggregate_error_with_signs', round(r.aggregate_error_with_signs::numeric, 6),
            'pass', r.pass,
            'num_bin_used', r.num_bin_used
        );

        passed := r.leg IS NOT NULL AND got = expected;

        RETURN NEXT;
    END LOOP;

    DELETE FROM miovision_validation.mio_spec_processed_counts
    WHERE (intersection_uid, count_id) = (-1, -1);
    DELETE FROM miovision_validation.spectrum_studies
    WHERE (intersection_uid, count_id) = (-1, -1);
END;
$$;

--one failure!
SELECT * FROM gwolofs.test_spec_error_agg_mvmt();

--need to verify, but think these should be failures:
SELECT * FROM miovision_validation.spec_error_agg_mvmt WHERE vol_spec = 0 AND vol_mio > 0;
SELECT * FROM miovision_validation.spec_error_agg_leg WHERE vol_spec = 0 AND vol_mio > 0;
SELECT * FROM miovision_validation.spec_error_agg_exit WHERE vol_spec = 0 AND vol_mio > 0;