CREATE TABLE miovision_api.gapsize_lookup
(
    dt date NOT NULL,
    intersection_uid integer NOT NULL,
    classification_uid integer,
    hour_bin smallint NOT NULL,
    weekend boolean NOT NULL,
    avg_hour_vol numeric,
    gap_tolerance smallint,
    CONSTRAINT gapsize_lookup_unique
    UNIQUE NULLS NOT DISTINCT (dt, intersection_uid, hour_bin, classification_uid)
);

CREATE INDEX ON miovision_api.gapsize_lookup USING brin (dt);

ALTER TABLE miovision_api.gapsize_lookup
OWNER TO miovision_admins;

GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.gapsize_lookup
TO bdit_humans WITH GRANT OPTION;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE miovision_api.gapsize_lookup
TO miovision_api_bot;