CREATE OR REPLACE FUNCTION miovision_validation.int_array_sum(int[])
RETURNS bigint
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT coalesce(sum(x), 0)::bigint FROM unnest($1) AS x $$;


DROP TABLE IF EXISTS miovision_validation.validation_scenarios;
CREATE TABLE miovision_validation.validation_scenarios (
    scenario_name text NOT NULL,
    -- which view this scenario validates against
    validation_rule text NOT NULL,
    description text,
    -- fixture attributes (one row per movement; no defaults)
    leg text NOT NULL,
    exit_leg text,
    spec_class text NOT NULL,
    movement_name text NOT NULL,
    spec_movements text[] NOT NULL,
    classification_uids int[] NOT NULL,
    movement_uids int[] NOT NULL,
    -- one element per bin (max 24 with the current datetime_bin logic)
    spec_counts int[] NOT NULL,
    mio_counts int[] NOT NULL,
    -- expected output of the validation_rule view (per movement for mvmt, per leg for leg)
    exp_vol_spec bigint GENERATED ALWAYS AS (miovision_validation.int_array_sum(spec_counts)) STORED,
    exp_vol_mio bigint GENERATED ALWAYS AS (miovision_validation.int_array_sum(mio_counts)) STORED,
    exp_err numeric,
    exp_pass boolean,
    PRIMARY KEY (scenario_name, validation_rule, leg, spec_class, movement_name),
    CONSTRAINT equal_length_counts CHECK (cardinality(spec_counts) = cardinality(mio_counts)),
    CONSTRAINT valid_validation_rule CHECK (validation_rule IN (
        'spec_error_agg_mvmt', 'spec_error_agg_leg', 'spec_error_agg_exit', 'spec_error_percentile_mvmt'
    ))
);

TRUNCATE miovision_validation.validation_scenarios;

--aggregate movement level tests
INSERT INTO miovision_validation.validation_scenarios (
    scenario_name, validation_rule, description, leg, spec_class, movement_name,
    spec_movements, classification_uids, movement_uids,
    spec_counts, mio_counts, exp_err, exp_pass
)
SELECT
    v.scenario_name, 'spec_error_agg_mvmt', v.description, 'N', 'vehicle_light', 'Left',
    '{n_cars_l}'::text[], '{1,8}'::int[], '{2}'::int[],
    v.spec_counts, v.mio_counts, v.exp_err, v.exp_pass
FROM (VALUES
    ('mio_5pct_higher', 'Miovision 5% higher than Spectrum (100 vs 105)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[11, 21, 31, 26, 16], 0.05, true),
    ('mio_5pct_lower', 'Miovision 5% lower than Spectrum (100 vs 95)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[9, 19, 29, 24, 14], -0.05, true),
    ('mio_10pct_higher', 'Miovision 10% higher than Spectrum (100 vs 110)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[12, 22, 32, 27, 17], 0.10, false),
    ('mio_10pct_lower', 'Miovision 10% lower than Spectrum (100 vs 90)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[8, 18, 28, 23, 13], -0.10, false),
    ('spec_zero_mio_100', 'Spectrum all zeros, Miovision totals 100 (division by zero)',
        ARRAY[0, 0, 0, 0, 0], ARRAY[20, 20, 20, 20, 20], NULL, false),
    ('both_zero', 'Both sources zero in every bin',
        ARRAY[0, 0, 0, 0, 0], ARRAY[0, 0, 0, 0, 0], NULL, true),
    ('mio_zero_spec_100', 'Miovision all zeros, Spectrum totals 100 (error of -100%)',
        ARRAY[20, 20, 20, 20, 20], ARRAY[0, 0, 0, 0, 0], -1, false)
) AS v(scenario_name, description, spec_counts, mio_counts, exp_err, exp_pass);

--percentile movement level tests
INSERT INTO miovision_validation.validation_scenarios (
    scenario_name, validation_rule, description, leg, spec_class, movement_name,
    spec_movements, classification_uids, movement_uids,
    spec_counts, mio_counts, exp_err, exp_pass
)
SELECT
    v.scenario_name, 'spec_error_percentile_mvmt', v.description, 'N', 'vehicle_light', 'Left',
    '{n_cars_l}'::text[], '{1,8}'::int[], '{2}'::int[],
    v.spec_counts, v.mio_counts, v.exp_err, v.exp_pass
FROM (VALUES
    ('mio_5pct_higher', 'Miovision 5% higher than Spectrum (100 vs 105)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[11, 21, 31, 26, 16], 0.05, true),
    ('mio_5pct_lower', 'Miovision 5% lower than Spectrum (100 vs 95)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[9, 19, 29, 24, 14], -0.05, true),
    ('mio_10pct_higher', 'Miovision 10% higher than Spectrum (100 vs 110)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[12, 22, 32, 27, 17], 0.10, false),
    ('mio_10pct_lower', 'Miovision 10% lower than Spectrum (100 vs 90)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[8, 18, 28, 23, 13], -0.10, false),
    ('spec_zero_mio_100', 'Spectrum all zeros, Miovision totals 100 (division by zero)',
        ARRAY[0, 0, 0, 0, 0], ARRAY[20, 20, 20, 20, 20], NULL, false),
    ('both_zero', 'Both sources zero in every bin',
        ARRAY[0, 0, 0, 0, 0], ARRAY[0, 0, 0, 0, 0], NULL, true),
    ('mio_zero_spec_100', 'Miovision all zeros, Spectrum totals 100 (error of -100%)',
        ARRAY[20, 20, 20, 20, 20], ARRAY[0, 0, 0, 0, 0], -1, false)
) AS v(scenario_name, description, spec_counts, mio_counts, exp_err, exp_pass);


--leg level tests (exp_err, exp_pass are the LEG's values, repeated on each movement row)
INSERT INTO miovision_validation.validation_scenarios (
    scenario_name, validation_rule, description, leg, spec_class, movement_name,
    spec_movements, classification_uids, movement_uids,
    spec_counts, mio_counts, exp_err, exp_pass
)
VALUES
       
    ('leg_n_offsetting_errors', 'spec_error_agg_leg', 'N leg: Left +10%, Thru -10%, Right exact, errors cancel (300 vs 300)',
        'N', 'vehicle_light', 'Left',
        '{n_cars_l}', '{1,8}', '{2}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[12, 22, 32, 27, 17], 0, true),
    ('leg_n_offsetting_errors', 'spec_error_agg_leg', 'N leg: Left +10%, Thru -10%, Right exact, errors cancel (300 vs 300)',
        'N', 'vehicle_light', 'Thru',
        '{n_cars_t}', '{1,8}', '{1}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[8, 18, 28, 23, 13], 0, true),
    ('leg_n_offsetting_errors', 'spec_error_agg_leg', 'N leg: Left +10%, Thru -10%, Right exact, errors cancel (300 vs 300)',
        'N', 'vehicle_light', 'Right',
        '{n_cars_r}', '{1,8}', '{3}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[10, 20, 30, 25, 15], 0, true),

    ('leg_n_all_10pct_higher', 'spec_error_agg_leg', 'N leg: every movement +10% (300 vs 330)',
        'N', 'vehicle_light', 'Left',
        '{n_cars_l}', '{1,8}', '{2}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[12, 22, 32, 27, 17], 0.10, false),
    ('leg_n_all_10pct_higher', 'spec_error_agg_leg', 'N leg: every movement +10% (300 vs 330)',
        'N', 'vehicle_light', 'Thru',
        '{n_cars_t}', '{1,8}', '{1}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[12, 22, 32, 27, 17], 0.10, false),
    ('leg_n_all_10pct_higher', 'spec_error_agg_leg', 'N leg: every movement +10% (300 vs 330)',
        'N', 'vehicle_light', 'Right',
        '{n_cars_r}', '{1,8}', '{3}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[12, 22, 32, 27, 17], 0.10, false);
        
INSERT INTO miovision_validation.validation_scenarios (
    scenario_name, validation_rule, description, leg, exit_leg, spec_class, movement_name,
    spec_movements, classification_uids, movement_uids,
    spec_counts, mio_counts, exp_err, exp_pass
)
VALUES
    -- N Thru + E Left both exit on the S leg
    ('exit_s_offsetting_errors', 'spec_error_agg_exit', 'S exit: N Thru +10%, E Left -10%, errors cancel (200 vs 200)',
        'N', 'S', 'vehicle_light', 'Thru',
        '{n_cars_t}', '{1,8}', '{1}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[12, 22, 32, 27, 17], 0, true),
    ('exit_s_offsetting_errors', 'spec_error_agg_exit', 'S exit: N Thru +10%, E Left -10%, errors cancel (200 vs 200)',
        'E', 'S', 'vehicle_light', 'Left',
        '{e_cars_l}', '{1,8}', '{2}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[8, 18, 28, 23, 13], 0, true),
 
    ('exit_s_all_10pct_higher', 'spec_error_agg_exit', 'S exit: N Thru and E Left both +10% (200 vs 220)',
        'N', 'S', 'vehicle_light', 'Thru',
        '{n_cars_t}', '{1,8}', '{1}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[12, 22, 32, 27, 17], 0.10, false),
    ('exit_s_all_10pct_higher', 'spec_error_agg_exit', 'S exit: N Thru and E Left both +10% (200 vs 220)',
        'E', 'S', 'vehicle_light', 'Left',
        '{e_cars_l}', '{1,8}', '{2}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[12, 22, 32, 27, 17], 0.10, false),

    --miovision all zeroes
    ('exit_s_mio_zero', 'spec_error_agg_exit', 'S exit: Miovision counts all zero',
        'N', 'S', 'vehicle_light', 'Thru',
        '{n_cars_t}', '{1,8}', '{1}',
        ARRAY[0, 0, 0, 0, 0], ARRAY[12, 22, 32, 27, 17], null, false),
    ('exit_s_mio_zero', 'spec_error_agg_exit', 'S exit: Miovision counts all zero',
        'E', 'S', 'vehicle_light', 'Left',
        '{e_cars_l}', '{1,8}', '{2}',
        ARRAY[0, 0, 0, 0, 0], ARRAY[12, 22, 32, 27, 17], null, false),
    
    --spectrum all zeroes
    ('exit_s_spec_zero', 'spec_error_agg_exit', 'S exit: Spectrum counts all zero',
        'N', 'S', 'vehicle_light', 'Thru',
        '{n_cars_t}', '{1,8}', '{1}',
        ARRAY[12, 22, 32, 27, 17], ARRAY[0, 0, 0, 0, 0], null, false),
    ('exit_s_spec_zero', 'spec_error_agg_exit', 'S exit: Spectrum counts all zero',
        'E', 'S', 'vehicle_light', 'Left',
        '{e_cars_l}', '{1,8}', '{2}',
        ARRAY[12, 22, 32, 27, 17], ARRAY[0, 0, 0, 0, 0], null, false),
 
    -- three entrances feed the S exit (N Thru, E Left, W Right), -5% overall
    ('exit_s_three_movements', 'spec_error_agg_exit', 'S exit: N Thru + E Left + W Right, 300 vs 285 (-5%)',
        'N', 'S', 'vehicle_light', 'Thru',
        '{n_cars_t}', '{1,8}', '{1}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[9, 19, 29, 24, 14], -0.05, true),
    ('exit_s_three_movements', 'spec_error_agg_exit', 'S exit: N Thru + E Left + W Right, 300 vs 285 (-5%)',
        'E', 'S', 'vehicle_light', 'Left',
        '{e_cars_l}', '{1,8}', '{2}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[9, 19, 29, 24, 14], -0.05, true),
    ('exit_s_three_movements', 'spec_error_agg_exit', 'S exit: N Thru + E Left + W Right, 300 vs 285 (-5%)',
        'W', 'S', 'vehicle_light', 'Right',
        '{w_cars_r}', '{1,8}', '{3}',
        ARRAY[10, 20, 30, 25, 15], ARRAY[9, 19, 29, 24, 14], -0.05, true);