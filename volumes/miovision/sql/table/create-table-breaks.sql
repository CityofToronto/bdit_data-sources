CREATE TABLE miovision_api.breaks (
    uid integer NOT NULL DEFAULT nextval('miovision_api.breaks_uid_seq'::regclass),
    intersection_uid integer,
    classification_uid integer,
    -- moment the change takes place
    break_time timestamp NOT NULL,
    -- approximate bounds if the precise time is not known
    give_or_take interval,
    -- required description of what changed - be verbose!
    notes text NOT NULL,
    CONSTRAINT miovision_breaks_pkey PRIMARY KEY (uid),
    CONSTRAINT miovision_breaks_intersection_uid_classification_uid_break_key
    UNIQUE NULLS NOT DISTINCT (intersection_uid, classification_uid, break_time),
    CONSTRAINT miovision_breaks_classification_uid_fkey FOREIGN KEY (classification_uid)
    REFERENCES miovision_api.classifications (classification_uid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION,
    CONSTRAINT miovision_breaks_intersection_uid_fkey FOREIGN KEY (intersection_uid)
    REFERENCES miovision_api.intersections (intersection_uid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
);

ALTER TABLE miovision_api.breaks OWNER TO miovision_admins;

GRANT ALL ON TABLE miovision_api.breaks TO miovision_data_detectives;
GRANT ALL ON SEQUENCE miovision_api.breaks_uid_seq TO miovision_data_detectives;

GRANT SELECT ON miovision_api.breaks TO bdit_humans;

CREATE OR REPLACE TRIGGER audit_trigger_row
AFTER INSERT OR DELETE OR UPDATE
ON miovision_api.breaks
FOR EACH ROW
EXECUTE FUNCTION miovision_api.if_modified_func('true');

CREATE OR REPLACE TRIGGER audit_trigger_stm
AFTER TRUNCATE
ON miovision_api.breaks
FOR EACH STATEMENT
EXECUTE FUNCTION miovision_api.if_modified_func('true');

COMMENT ON TABLE miovision_api.breaks
IS 'Moments in time when data collection methods changed in such
a way that we would expect clear pre- and post-change paradigms
that may not be intercomparable.';

COMMENT ON COLUMN miovision_api.breaks.intersection_uid
IS 'NULLs are allowed in the column intersection_uid to denote
breaks that affect all intersections.';

COMMENT ON COLUMN miovision_api.breaks.classification_uid
IS 'NULLs are allowed in the column classification_uid to denote
breaks that affect all classifications.';
