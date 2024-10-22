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

/*
--initial code used to populate

INSERT INTO ecocounter.sensitivity_history (flow_id, sensitivity, date_range)

SELECT
    flow_id,
    regexp_substr(reverse(setting), '[0-9]', 1)::int AS sensitivity, #the second number
    daterange(date_of_change, null)
FROM ecocounter.sensitivity_changes
WHERE uid IN (16,17,18,19,20,21,22) --the ones that were like #->#
UNION
SELECT
    flow_id,
    regexp_substr(setting, '[0-9]', 1)::int AS sensitivity, #the first number
    daterange(null, date_of_change)
FROM ecocounter.sensitivity_changes
WHERE uid IN (16,17,18,19,20,21,22)
*/