-- Table: ecocounter.sensitivity_history
-- DROP TABLE IF EXISTS ecocounter.sensitivity_history;

CREATE TABLE IF NOT EXISTS ecocounter.sensitivity_history
(
    flow_id numeric,
    sensitivity text,
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

WITH dates AS (
    SELECT flow_id, date_of_change::date, setting
    FROM ecocounter.sensitivity_changes
    UNION 
    SELECT flow_id, flows.first_active::date, 'Original configuration'
    FROM ecocounter.flows
)

SELECT
    flow_id,
    daterange(
        date_of_change,
        LEAD(date_of_change) OVER w
    ) AS date_range,
    setting
FROM dates
WINDOW w AS (PARTITION BY flow_id ORDER BY date_of_change)
*/