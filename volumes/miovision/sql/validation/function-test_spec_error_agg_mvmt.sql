DROP FUNCTION IF EXISTS miovision_validation.test_spec_error_agg_mvmt;
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
        WHERE
            vs.scenario_name = sc.scenario_name
            AND vs.validation_rule = sc.validation_rule;

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
                        'pass', e.exp_pass) END AS exp_json,
                    CASE WHEN v.leg IS NOT NULL THEN jsonb_build_object(
                        'vol_spec', v.vol_spec,
                        'vol_mio', v.vol_mio,
                        'aggregate_error_with_signs', round(v.aggregate_error_with_signs::numeric, 6),
                        'pass', v.pass) END AS got_json
                FROM miovision_validation.validation_scenarios AS e
                LEFT JOIN miovision_validation.spec_error_agg_mvmt AS v
                    ON v.leg = e.leg
                    AND v.classification = e.spec_class
                    AND v.movement = e.movement_name
                    AND (v.intersection_uid, v.count_id) = (-1, -1)
                WHERE
                    e.scenario_name = sc.scenario_name
                    AND e.validation_rule = sc.validation_rule
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

        ELSIF sc.validation_rule = 'spec_error_percentile_mvmt' THEN
            FOR r IN
                SELECT
                    e.scenario_name AS e_scenario,
                    coalesce(e.leg, v.leg) AS leg,
                    coalesce(e.spec_class, v.classification) AS classification,
                    coalesce(e.movement_name, v.movement) AS movement,
                    CASE WHEN e.scenario_name IS NOT NULL THEN jsonb_build_object(
                        'vol_spec', e.exp_vol_spec,
                        'vol_mio', e.exp_vol_mio,
                        'percentile_80_r', round(e.exp_err, 6),
                        'pass', e.exp_pass) END AS exp_json,
                    CASE WHEN v.leg IS NOT NULL THEN jsonb_build_object(
                        'vol_spec', v.spectrum_volume,
                        'vol_mio', v.miovision_volume,
                        'percentile_80_r', round(v.percentile_80_r::numeric, 6),
                        'pass', v.pass_80th_percentile_r) END AS got_json
                FROM miovision_validation.validation_scenarios AS e
                LEFT JOIN miovision_validation.spec_error_percentile_mvmt AS v
                    ON v.leg = e.leg
                    AND v.classification = e.spec_class
                    AND v.movement = e.movement_name
                    AND (v.intersection_uid, v.count_id) = (-1, -1)
                WHERE
                    e.scenario_name = sc.scenario_name
                    AND e.validation_rule = sc.validation_rule
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

        ELSIF sc.validation_rule = 'spec_error_agg_exit' THEN
            FOR r IN
                WITH validation_scenario AS (
                    SELECT
                        vs.scenario_name,
                        vs.exit_leg,
                        vs.spec_class,
                        vs.exp_err,
                        vs.exp_pass,
                        sum(vs.exp_vol_spec) AS exp_vol_spec,
                        sum(vs.exp_vol_mio) AS exp_vol_mio
                    FROM miovision_validation.validation_scenarios AS vs
                    WHERE
                        vs.scenario_name = sc.scenario_name
                        AND vs.validation_rule = sc.validation_rule
                    GROUP BY
                        vs.scenario_name,
                        vs.exit_leg,
                        vs.spec_class,
                        vs.exp_err,
                        vs.exp_pass
                )

                SELECT
                    e.scenario_name AS e_scenario,
                    coalesce(e.exit_leg, v.leg) AS leg,
                    coalesce(e.spec_class, v.classification) AS classification,
                    CASE WHEN e.scenario_name IS NOT NULL THEN jsonb_build_object(
                        'vol_spec', e.exp_vol_spec,
                        'vol_mio', e.exp_vol_mio,
                        'aggregate_error_with_signs', round(e.exp_err, 6),
                        'pass', e.exp_pass) END AS exp_json,
                    CASE WHEN v.leg IS NOT NULL THEN jsonb_build_object(
                        'vol_spec', v.vol_spec,
                        'vol_mio', v.vol_mio,
                        'aggregate_error_with_signs', round(v.aggregate_error_with_signs::numeric, 6),
                        'pass', v.pass) END AS got_json
                FROM validation_scenario AS e
                LEFT JOIN miovision_validation.spec_error_agg_exit AS v
                    ON v.leg = e.exit_leg
                    AND v.classification = e.spec_class
                    AND (v.intersection_uid, v.count_id) = (-1, -1)
                ORDER BY 2, 3
            LOOP
                scenario := sc.scenario_name;
                validation_rule := sc.validation_rule;
                leg := r.leg;            -- exit leg
                classification := r.classification;
                movement := NULL;
                expected := r.exp_json;
                got := r.got_json;
                passed := r.exp_json IS NOT NULL AND r.got_json IS NOT NULL AND r.exp_json = r.got_json;
                RETURN NEXT;
            END LOOP;

        ELSIF sc.validation_rule = 'spec_error_agg_leg' THEN
            FOR r IN
                WITH validation_scenario AS (
                    SELECT
                        vs.scenario_name,
                        vs.description,
                        vs.leg,
                        vs.spec_class,
                        vs.classification_uids,
                        vs.exp_err,
                        vs.exp_pass,
                        sum(vs.exp_vol_spec) AS exp_vol_spec,
                        sum(vs.exp_vol_mio) AS exp_vol_mio
                    FROM miovision_validation.validation_scenarios AS vs
                    WHERE
                        vs.scenario_name = sc.scenario_name
                        AND vs.validation_rule = sc.validation_rule
                    GROUP BY
                        vs.scenario_name,
                        vs.description,
                        vs.leg,
                        vs.spec_class,
                        vs.classification_uids,
                        vs.exp_err,
                        vs.exp_pass
                )

                SELECT
                    e.scenario_name AS e_scenario,
                    coalesce(e.leg, v.leg) AS leg,
                    coalesce(e.spec_class, v.classification) AS classification,
                    CASE WHEN e.scenario_name IS NOT NULL THEN jsonb_build_object(
                        'vol_spec', e.exp_vol_spec,
                        'vol_mio', e.exp_vol_mio,
                        'aggregate_error_with_signs', round(e.exp_err, 6),
                        'pass', e.exp_pass) END AS exp_json,
                    CASE WHEN v.leg IS NOT NULL THEN jsonb_build_object(
                        'vol_spec', v.vol_spec,
                        'vol_mio', v.vol_mio,
                        'aggregate_error_with_signs', round(v.aggregate_error_with_signs::numeric, 6),
                        'pass', v.pass) END AS got_json
                FROM validation_scenario AS e
                LEFT JOIN miovision_validation.spec_error_agg_leg AS v
                    ON v.leg = e.leg
                    AND v.classification = e.spec_class
                    AND (v.intersection_uid, v.count_id) = (-1, -1)
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

SELECT * FROM miovision_validation.test_spec_error_agg_mvmt() WHERE passed IS False