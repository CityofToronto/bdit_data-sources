CREATE TABLE miovision_api.discontinuities (
    uid serial PRIMARY KEY,
    intersection_uid integer,
    classification_uid integer,
    -- moment the change takes place
    break timestamp NOT NULL,
    -- approximate bounds if the precise time is not known
    give_or_take interval,
    -- required description of what changed - be verbose!
    notes text NOT NULL
);

ALTER TABLE miovision_api.discontinuities OWNER TO miovision_admins;

GRANT ALL ON TABLE miovision_api.discontinuities TO miovision_data_detectives;

GRANT SELECT ON miovision_api.discontinuities TO bdit_humans;

CREATE OR REPLACE TRIGGER audit_trigger_row
AFTER INSERT OR DELETE OR UPDATE
ON miovision_api.discontinuities
FOR EACH ROW
EXECUTE FUNCTION miovision_api.if_modified_func('true');

CREATE OR REPLACE TRIGGER audit_trigger_stm
AFTER TRUNCATE
ON miovision_api.discontinuities
FOR EACH STATEMENT
EXECUTE FUNCTION miovision_api.if_modified_func('true');

COMMENT ON TABLE miovision_api.discontinuities
IS 'Moments in time when data collection methods changed in such
a way that we would expect clear pre- and post-change paradigms
that may not be intercomparable.';

COMMENT ON COLUMN miovision_api.discontinuities
IS 'NULLs are allowed in the column intersection_uid to denote
discontinuities that affect all intersections.';

COMMENT ON COLUMN miovision_api.discontinuities
IS 'NULLs are allowed in the column classification_uid to denote
discontinuities that affect all classifications.';