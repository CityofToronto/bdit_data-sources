DROP VIEW IF EXISTS here.ta_path_combined;
CREATE OR REPLACE VIEW here.ta_path_combined AS
SELECT link_dir, tx::date AS dt, tx, mean, harmonic_mean, stddev, min_spd, max_spd, pct_50, pct_85, sample_size
FROM here.ta_path_hm
WHERE tx >= '2023-12-13'
UNION
SELECT link_dir, dt, tx, mean, NULL, stddev, min_spd, max_spd, pct_50, pct_85, sample_size
FROM here.ta_path
WHERE dt < '2023-12-13';