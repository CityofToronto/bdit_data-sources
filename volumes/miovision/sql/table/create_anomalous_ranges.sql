CREATE TABLE miovision_api.anomalous_ranges (
    uid serial PRIMARY KEY,
    intersection_uid smallint,
    classification_uid smallint,
    time_range tsrange NOT NULL,
    range_start timestamp NOT NULL,
    range_end timestamp,
    notes text NOT NULL,
    investigation_level text NOT NULL,
    problem_level text NOT NULL,
    CONSTRAINT miovision_qc_pkey PRIMARY KEY (uid),
    CONSTRAINT miovision_qc_intersection_uid_classification_uid_time_range_key
    UNIQUE (intersection_uid, classification_uid, range_start, range_end),
    CONSTRAINT miovision_qc_classification_uid_fkey FOREIGN KEY (classification_uid)
        REFERENCES miovision_api.classifications (classification_uid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT miovision_qc_intersection_uid_fkey FOREIGN KEY (intersection_uid)
        REFERENCES miovision_api.intersections (intersection_uid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT miovision_qc_problem_level_fkey FOREIGN KEY (problem_level)
        REFERENCES miovision_api.anomaly_problem_levels (uid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT miovision_qc_qa_level_fkey FOREIGN KEY (investigation_level)
        REFERENCES miovision_api.anomaly_investigation_levels (uid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT anomalous_ranges_end CHECK (
        range_end = upper(time_range) OR (range_end IS NULL AND upper(time_range) IS NULL)
    ),
    CONSTRAINT anomalous_ranges_range_order CHECK (
        range_start < range_end OR range_end IS NULL
    ),
    CONSTRAINT anomalous_ranges_start CHECK (
        range_start = lower(time_range)
    )
);

ALTER TABLE IF EXISTS miovision_api.anomalous_ranges
OWNER TO miovision_admins;

REVOKE ALL ON TABLE miovision_api.anomalous_ranges FROM bdit_humans;

GRANT SELECT ON TABLE miovision_api.anomalous_ranges TO bdit_humans;

GRANT ALL ON TABLE miovision_api.anomalous_ranges TO miovision_admins;

GRANT ALL ON TABLE miovision_api.anomalous_ranges TO miovision_data_detectives;

GRANT ALL ON TABLE miovision_api.anomalous_ranges TO nwessel;

-- Trigger: audit_trigger_row

-- DROP TRIGGER IF EXISTS audit_trigger_row ON miovision_api.anomalous_ranges;

CREATE OR REPLACE TRIGGER audit_trigger_row
    AFTER INSERT OR DELETE OR UPDATE 
    ON miovision_api.anomalous_ranges
    FOR EACH ROW
    EXECUTE FUNCTION miovision_api.if_modified_func('true');

-- Trigger: audit_trigger_stm

-- DROP TRIGGER IF EXISTS audit_trigger_stm ON miovision_api.anomalous_ranges;

CREATE OR REPLACE TRIGGER audit_trigger_stm
    AFTER TRUNCATE
    ON miovision_api.anomalous_ranges
    FOR EACH STATEMENT
    EXECUTE FUNCTION miovision_api.if_modified_func('true');