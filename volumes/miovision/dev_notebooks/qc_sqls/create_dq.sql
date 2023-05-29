-- Make a table to store time ranges with bad/questionable/susicious data
-- TODO:
/*
ALTER TABLE scannon.miovision_qc SET SCHEMA miovision_api;
ALTER TABLE miovision_api.miovision_qc RENAME qa;
ALTER TABLE nwessel.qc_investigation_levels SET SCHEMA miovision_api;
ALTER TABLE nwessel.qc_problem_levels SET SCHEMA miovision_api;

CREATE FUNCTION miovision_api.update_qa_last_updated_ts()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_qa_last_updated_ts_trigger
BEFORE UPDATE ON miovision_api.qa
FOR EACH ROW
EXECUTE PROCEDURE miovision_api.update_qa_last_updated_ts();
*/

CREATE TABLE nwessel.qa_investigation_levels (
    uid text PRIMARY KEY,
    description text UNIQUE NOT NULL
);
COMMENT ON TABLE nwessel.qa_investigation_levels
IS 'Indicates the furthest degree to which the movision QA issue has been investigated. Is this only a suspicion? Or has the issue been fully confirmed/resolved?';

CREATE TABLE nwessel.qa_problem_levels (
    uid text PRIMARY KEY,
    description text UNIQUE NOT NULL
);
COMMENT ON TABLE nwessel.qa_problem_levels
IS 'What is the nature of the problem indicated for the given subset of miovision data?';

CREATE TABLE scannon.miovision_qc (
    uid serial PRIMARY KEY,
    intersection_uid smallint REFERENCES miovision_api.intersections (intersection_uid),
    intersection_name text,
    classification_uid smallint REFERENCES miovision_api.classifications (classification_uid),
    last_updated timestamp DEFAULT NOW(),
    time_range tsrange NOT NULL,
    notes text,
    investigation_level text NOT NULL REFERENCES nwessel.qa_investigation_levels (uid),
    problem_level text NOT NULL REFERENCES nwessel.qa_problem_levels (uid),
    UNIQUE (intersection_uid, classification_uid, time_range)
);

-- this table was initially populated using data from mio_dq_notes thusly:
/*
INSERT INTO scannon.miovision_qc
SELECT
    mn.intersection_uid::bigint,
    mn.intersection_name,
    1 AS classification_uid,
    now() AS last_updated,
    mn.excl_range::tsrange AS time_range,
    mn.notes AS notes
FROM scannon.mio_dq_notes AS mn;
*/