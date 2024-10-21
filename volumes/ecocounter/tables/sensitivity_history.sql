-- Table: ecocounter.sensitivity_history
-- DROP TABLE IF EXISTS ecocounter.sensitivity_history;

CREATE TABLE IF NOT EXISTS ecocounter.sensitivity_history
(
    flow_id numeric,
    sensitivity integer,
    date_range daterange,
    CONSTRAINT eco_sensitivity_exclude EXCLUDE USING gist (
        date_range WITH &&,
        flow_id WITH =
    )
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecocounter.sensitivity_history
OWNER TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.sensitivity_history FROM bdit_humans;

GRANT SELECT ON TABLE ecocounter.sensitivity_history TO bdit_humans;

GRANT ALL ON TABLE ecocounter.sensitivity_history TO ecocounter_admins;