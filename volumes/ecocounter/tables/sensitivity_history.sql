-- Table: ecocounter.sensitivity_history
-- DROP TABLE IF EXISTS ecocounter.sensitivity_history;

CREATE TABLE IF NOT EXISTS ecocounter.sensitivity_history
(
    flow_id numeric,
    date_range daterange,
    setting text,
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

COMMENT ON TABLE ecocounter.sensitivity_history IS
'Stores sensitivity adjustments for each flow as date ranges to '
'ranges when data is comparable.';

-- Trigger: audit_trigger_row

-- DROP TRIGGER IF EXISTS audit_trigger_row ON ecocounter.sensitivity_history;

CREATE OR REPLACE TRIGGER audit_trigger_row
AFTER INSERT OR DELETE OR UPDATE 
ON ecocounter.sensitivity_history
FOR EACH ROW
EXECUTE FUNCTION ecocounter.if_modified_func('true');

-- Trigger: audit_trigger_stm

-- DROP TRIGGER IF EXISTS audit_trigger_stm ON ecocounter.sensitivity_history;

CREATE OR REPLACE TRIGGER audit_trigger_stm
AFTER TRUNCATE
ON ecocounter.sensitivity_history
FOR EACH STATEMENT
EXECUTE FUNCTION ecocounter.if_modified_func('true');

/*
--initial code used to populate

WITH dates AS (
    SELECT flow_id, date_of_change::date, setting
    FROM ecocounter.sensitivity_changes
    UNION
    SELECT flow_id, flows.first_active::date, 'Original configuration'
    FROM ecocounter.flows
)

INSERT INTO ecocounter.sensitivity_history (flow_id, date_range, setting)
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