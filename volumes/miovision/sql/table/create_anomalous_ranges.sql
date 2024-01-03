-- Make a table to store time ranges with anomalous data
-- TODO:
/*
ALTER TABLE scannon.anomalous_ranges SET SCHEMA miovision_api;
ALTER TABLE nwessel.anomaly_investigation_levels SET SCHEMA miovision_api;
ALTER TABLE nwessel.anomaly_problem_levels SET SCHEMA miovision_api;

Also need to log changes to all three tables following the procedure documented here:
https://github.com/Toronto-Big-Data-Innovation-Team/bdit_audit-trigger
Not sure though if this should reference the `gis` schema or have this logging set 
up in `miovision_api`.
*/

CREATE TABLE miovision_api.anomaly_investigation_levels (
    uid text PRIMARY KEY,
    description text UNIQUE NOT NULL
);
COMMENT ON TABLE miovision_api.anomaly_investigation_levels
IS 'Indicates the furthest degree to which the movision QA issue has been investigated. Is this only a suspicion? Or has the issue been fully confirmed/resolved?';

CREATE TABLE miovision_api.anomaly_problem_levels (
    uid text PRIMARY KEY,
    description text UNIQUE NOT NULL
);
COMMENT ON TABLE miovision_api.anomaly_problem_levels
IS 'What is the nature of the problem indicated for the given subset of miovision data?';

CREATE TABLE miovision_api.anomalous_ranges (
    uid serial PRIMARY KEY,
    intersection_uid smallint REFERENCES miovision_api.intersections (intersection_uid),
    classification_uid smallint REFERENCES miovision_api.classifications (classification_uid),
    time_range tsrange NOT NULL,
    notes text NOT NULL,
    investigation_level text NOT NULL REFERENCES miovision_api.anomaly_investigation_levels (uid),
    problem_level text NOT NULL REFERENCES miovision_api.anomaly_problem_levels (uid),
    UNIQUE (intersection_uid, classification_uid, time_range)
);