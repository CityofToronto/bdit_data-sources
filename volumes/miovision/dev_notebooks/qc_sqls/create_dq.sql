-- make a table to store time ranges with bad data
CREATE TABLE scannon.miovision_qc (
    intersection_uid smallint REFERENCES miovision_api.intersections (intersection_uid),
    intersection_name text,
    classification_uid smallint REFERENCES miovision_api.classifications (classification_uid),
    last_updated timestamp DEFAULT NOW(),
    time_range tsrange NOT NULL,
    notes text,
    UNIQUE (intersection_uid, classification_uid, time_range)
);

-- this table was populated using data from mio_dq_notes thusly:
INSERT INTO scannon.miovision_qc
SELECT
    mn.intersection_uid::bigint,
    mn.intersection_name,
    1 AS classification_uid,
    now() AS last_updated,
    mn.excl_range::tsrange AS time_range,
    mn.notes AS notes
FROM scannon.mio_dq_notes AS mn;