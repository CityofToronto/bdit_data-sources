-- View: here.ta_path_hm_view

-- DROP VIEW here.ta_path_hm_view;

CREATE OR REPLACE VIEW here.ta_path_hm_view AS
SELECT
    ta_path.link_dir,
    ta_path.tx,
    NULL::integer AS epoch_min,
    ta_path.sample_size,
    ta_path.mean,
    ta_path.harmonic_mean,
    ta_path.stddev,
    ta_path.min_spd,
    ta_path.max_spd,
    ta_path.pct_50,
    ta_path.pct_85
FROM here.ta_path_hm AS ta_path;

ALTER TABLE here.ta_path_hm_view OWNER TO here_admins;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE here.ta_path_hm_view TO bdit_humans WITH GRANT OPTION;
GRANT SELECT ON TABLE here.ta_path_hm_view TO bdit_humans;
GRANT SELECT ON TABLE here.ta_path_hm_view TO covid_admins;
GRANT ALL ON TABLE here.ta_path_hm_view TO here_admins;
GRANT INSERT, SELECT, TRIGGER, UPDATE ON TABLE here.ta_path_hm_view TO here_bot;
GRANT ALL ON TABLE here.ta_path_hm_view TO rds_superuser WITH GRANT OPTION;

CREATE OR REPLACE TRIGGER transform_trigger_path
    INSTEAD OF INSERT
    ON here.ta_path_hm_view
    FOR EACH ROW
    EXECUTE FUNCTION here.here_insert_trigger_hm_path();
