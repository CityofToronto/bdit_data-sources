CREATE TABLE ecocounter.anomalous_ranges (
    uid smallserial PRIMARY KEY,
    flow_id numeric REFERENCES ecocounter.flows_unfiltered (flow_id),
    site_id numeric REFERENCES ecocounter.sites_unfiltered (site_id),
    time_range tsrange NOT NULL,
    notes text NOT NULL,
    investigation_level text NOT NULL REFERENCES miovision_api.anomaly_investigation_levels,
    problem_level text NOT NULL REFERENCES miovision_api.anomaly_problem_levels,
    UNIQUE (flow_id, site_id, time_range, investigation_level, problem_level, notes),
    CHECK (flow_id IS NOT NULL OR site_id IS NOT NULL)
);

ALTER TABLE ecocounter.anomalous_ranges OWNER TO ecocounter_admins;

GRANT SELECT ON ecocounter.anomalous_ranges TO bdit_humans;
GRANT UPDATE, INSERT, SELECT, DELETE ON ecocounter.anomalous_ranges TO ecocounter_data_detectives;

-- Trigger: audit_trigger_row

-- DROP TRIGGER IF EXISTS audit_trigger_row ON ecocounter.anomalous_ranges;

CREATE OR REPLACE TRIGGER audit_trigger_row
AFTER INSERT OR DELETE OR UPDATE 
ON ecocounter.anomalous_ranges
FOR EACH ROW
EXECUTE FUNCTION ecocounter.if_modified_func('true');

-- Trigger: audit_trigger_stm

-- DROP TRIGGER IF EXISTS audit_trigger_stm ON ecocounter.anomalous_ranges;

CREATE OR REPLACE TRIGGER audit_trigger_stm
AFTER TRUNCATE
ON ecocounter.anomalous_ranges
FOR EACH STATEMENT
EXECUTE FUNCTION ecocounter.if_modified_func('true');
