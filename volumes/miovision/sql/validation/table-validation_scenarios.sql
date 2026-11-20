CREATE OR REPLACE FUNCTION miovision_validation.int_array_sum(int[])
RETURNS bigint
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT coalesce(sum(x), 0)::bigint FROM unnest($1) AS x $$;

DROP TABLE IF EXISTS miovision_validation.validation_scenarios;
CREATE TABLE miovision_validation.validation_scenarios (
    scenario_name text PRIMARY KEY,
    description text,
    spec_movements text[] NOT NULL DEFAULT '{n_cars_l}',
    leg text NOT NULL DEFAULT 'N',
    spec_class text NOT NULL DEFAULT 'vehicle_light',
    movement_name text NOT NULL DEFAULT 'Left',
    classification_uids int[] NOT NULL DEFAULT '{1,8}',
    movement_uids int[] NOT NULL DEFAULT '{2}',
    -- one element per bin (max 24 with the current datetime_bin logic)
    spec_counts int[] NOT NULL,
    mio_counts int[] NOT NULL,
    -- expected output of spec_error_agg_mvmt
    exp_vol_spec bigint generated always AS (miovision_validation.int_array_sum (spec_counts)) stored,
    exp_vol_mio bigint generated always AS (miovision_validation.int_array_sum (mio_counts)) stored,
    exp_bins int generated always AS (CARDINALITY(spec_counts)) stored,
    exp_err numeric,
    exp_pass boolean,
    CONSTRAINT equal_length_counts CHECK (CARDINALITY(spec_counts) = CARDINALITY(mio_counts))
);

TRUNCATE miovision_validation.validation_scenarios;

INSERT INTO miovision_validation.validation_scenarios (
    scenario_name, description, spec_counts, mio_counts, exp_err, exp_pass
)
VALUES
    ('mio_5pct_higher', 'Miovision 5% higher than Spectrum (100 vs 105)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[11, 21, 31, 26, 16],
        0.05, true),

    ('mio_5pct_lower', 'Miovision 5% lower than Spectrum (100 vs 95)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[9, 19, 29, 24, 14],
        -0.05, true),

    ('mio_10pct_higher', 'Miovision 10% higher than Spectrum (100 vs 110)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[12, 22, 32, 27, 17],
        0.10, false),

    ('mio_10pct_lower', 'Miovision 10% lower than Spectrum (100 vs 90)',
        ARRAY[10, 20, 30, 25, 15], ARRAY[8, 18, 28, 23, 13],
        -0.10, false),

    ('spec_zero_mio_100', 'Spectrum all zeros, Miovision totals 100 (division by zero)',
        ARRAY[0, 0, 0, 0, 0], ARRAY[20, 20, 20, 20, 20],
        NULL, false),
        
    ('both_zero', 'Both sources zero in every bin',
        ARRAY[0, 0, 0, 0, 0], ARRAY[0, 0, 0, 0, 0],
        NULL, true),
        
    ('mio_zero_spec_100', 'Miovision all zeros, Spectrum totals 100 (error of -100%)',
        ARRAY[20, 20, 20, 20, 20], ARRAY[0, 0, 0, 0, 0],
        -1, false);