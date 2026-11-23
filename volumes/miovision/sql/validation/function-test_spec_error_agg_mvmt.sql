DROP FUNCTION miovision_validation.test_spec_error_agg_mvmt;
CREATE OR REPLACE FUNCTION miovision_validation.test_spec_error_agg_mvmt()
RETURNS TABLE (
    scenario text,
    validation_rule text,
    leg text,
    classification text,
    movement text,
    passed boolean,
    got jsonb,
    expected jsonb
)
LANGUAGE plpgsql AS
$$
DECLARE
    sc record;
    r record;
BEGIN
    FOR sc IN
        SELECT DISTINCT vs.scenario_name, vs.validation_rule
        FROM miovision_validation.validation_scenarios AS vs
        ORDER BY 1
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
            vs.spec_movements, vs.leg, vs.spec_class, vs.movement_name,
            b.spec, b.mio,
            vs.classification_uids, vs.movement_uids
        FROM miovision_validation.validation_scenarios AS vs,
            LATERAL unnest(vs.spec_counts, vs.mio_counts) WITH ORDINALITY AS b(spec, mio, i)
        WHERE vs.scenario_name = sc.scenario_name;

        IF sc.validation_rule = 'spec_error_agg_mvmt' THEN
            FOR r IN
                SELECT
                    e.scenario_name AS e_scenario,
                    coalesce(e.leg, v.leg) AS leg,
                    coalesce(e.spec_class, v.classification) AS classification,
                    coalesce(e.movement_name, v.movement) AS movement,
                    CASE WHEN e.scenario_name IS NOT NULL THEN jsonb_build_object(
                        'vol_spec', e.exp_vol_spec,
                        'vol_mio', e.exp_vol_mio,
                        'aggregate_error_with_signs', round(e.exp_err, 6),
                        'pass', e.exp_pass,
                        'num_bin_used', e.exp_bins) END AS exp_json,
                    CASE WHEN v.leg IS NOT NULL THEN jsonb_build_object(
                        'vol_spec', v.vol_spec,
                        'vol_mio', v.vol_mio,
                        'aggregate_error_with_signs', round(v.aggregate_error_with_signs::numeric, 6),
                        'pass', v.pass,
                        'num_bin_used', v.num_bin_used) END AS got_json
                FROM (
                    SELECT * FROM miovision_validation.validation_scenarios
                    WHERE scenario_name = sc.scenario_name
                ) AS e
                FULL JOIN (
                    SELECT * FROM miovision_validation.spec_error_agg_mvmt
                    WHERE (intersection_uid, count_id) = (-1, -1)
                ) AS v
                    ON v.leg = e.leg
                    AND v.classification = e.spec_class
                    AND v.movement = e.movement_name
                ORDER BY 2, 3, 4
            LOOP
                scenario := sc.scenario_name;
                validation_rule := sc.validation_rule;
                leg := r.leg;
                classification := r.classification;
                movement := r.movement;
                expected := r.exp_json;
                got := r.got_json;
                passed := r.exp_json IS NOT NULL AND r.got_json IS NOT NULL AND r.exp_json = r.got_json;
                RETURN NEXT;
            END LOOP;

        ELSIF sc.validation_rule = 'spec_error_agg_leg' THEN
            FOR r IN
                SELECT
                    e.scenario_name AS e_scenario,
                    coalesce(e.leg, v.leg) AS leg,
                    coalesce(e.spec_class, v.classification) AS classification,
                    CASE WHEN e.scenario_name IS NOT NULL THEN jsonb_build_object(
                        'vol_spec', e.exp_vol_spec,
                        'vol_mio', e.exp_vol_mio,
                        'aggregate_error_with_signs', round(e.exp_err, 6),
                        'pass', e.exp_pass,
                        'num_bin_used', e.exp_bins) END AS exp_json,
                    CASE WHEN v.leg IS NOT NULL THEN jsonb_build_object(
                        'vol_spec', v.vol_spec,
                        'vol_mio', v.vol_mio,
                        'aggregate_error_with_signs', round(v.aggregate_error_with_signs::numeric, 6),
                        'pass', v.pass,
                        'num_bin_used', v.num_bin_used) END AS got_json
                FROM (
                    SELECT
                        validation_scenarios.scenario_name,
                        validation_scenarios.description,
                        validation_scenarios.leg,
                        validation_scenarios.spec_class,
                        validation_scenarios.classification_uids,
                        validation_scenarios.exp_err,
                        validation_scenarios.exp_bins,
                        validation_scenarios.exp_pass,
                        sum(validation_scenarios.exp_vol_spec) AS exp_vol_spec,
                        sum(validation_scenarios.exp_vol_mio) AS exp_vol_mio
                    FROM miovision_validation.validation_scenarios
                    WHERE validation_scenarios.scenario_name = sc.scenario_name
                    GROUP BY
                        validation_scenarios.scenario_name,
                        validation_scenarios.description,
                        validation_scenarios.leg,
                        validation_scenarios.spec_class,
                        validation_scenarios.classification_uids,
                        validation_scenarios.exp_err,
                        validation_scenarios.exp_bins,
                        validation_scenarios.exp_pass
                ) AS e
                FULL JOIN (
                    SELECT * FROM miovision_validation.spec_error_agg_leg
                    WHERE (intersection_uid, count_id) = (-1, -1)
                ) AS v
                    ON v.leg = e.leg
                    AND v.classification = e.spec_class
                ORDER BY 2, 3
            LOOP
                scenario := sc.scenario_name;
                validation_rule := sc.validation_rule;
                leg := r.leg;
                classification := r.classification;
                movement := NULL;
                expected := r.exp_json;
                got := r.got_json;
                passed := r.exp_json IS NOT NULL AND r.got_json IS NOT NULL AND r.exp_json = r.got_json;
                RETURN NEXT;
            END LOOP;
        END IF;
    END LOOP;

    DELETE FROM miovision_validation.mio_spec_processed_counts
    WHERE (intersection_uid, count_id) = (-1, -1);
    DELETE FROM miovision_validation.spectrum_studies
    WHERE (intersection_uid, count_id) = (-1, -1);
END;
$$;

SELECT * FROM miovision_validation.test_spec_error_agg_mvmt();