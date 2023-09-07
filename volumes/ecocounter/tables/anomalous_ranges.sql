CREATE TABLE ecocounter.anomalous_ranges (
    uid smallserial PRIMARY KEY,
    flow_id numeric REFERENCES ecocounter.flows (flow_id),
    site_id numeric REFERENCES ecocounter.sites (site_id),
    date_range daterange NOT NULL,
    notes text NOT NULL,
    investigation_level text NOT NULL REFERENCES miovision_api.anomaly_investigation_levels,
    problem_level text NOT NULL REFERENCES miovision_api.anomaly_problem_levels,
    UNIQUE (flow_id, site_id, date_range, investigation_level, problem_level, notes),
    CHECK (flow_id IS NOT NULL OR site_id IS NOT NULL)
);

ALTER TABLE ecocounter.anomalous_ranges OWNER TO ecocounter_admins;

GRANT SELECT ON ecocounter.anomalous_ranges TO bdit_humans;
