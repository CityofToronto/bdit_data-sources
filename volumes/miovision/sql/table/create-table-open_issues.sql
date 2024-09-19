-- Table: miovision_api.open_issues_review
-- DROP TABLE IF EXISTS miovision_api.open_issues_review;

CREATE TABLE IF NOT EXISTS miovision_api.open_issues_review
(
    uid smallint NOT NULL,
    intersection_uid smallint,
    intersection_id text COLLATE pg_catalog."default",
    intersection_name text COLLATE pg_catalog."default",
    classification_uid smallint,
    classification text COLLATE pg_catalog."default",
    leg text COLLATE pg_catalog."default",
    range_start date,
    num_days integer,
    notes text COLLATE pg_catalog."default",
    volume bigint,
    alerts text COLLATE pg_catalog."default",
    logged boolean,
    notes txt
    CONSTRAINT open_issues_review_pkey PRIMARY KEY (uid)
)

TABLESPACE pg_default;

ALTER TABLE miovision_api.open_issues_review OWNER TO miovision_api_bot;

GRANT ALL ON TABLE miovision_api.open_issues_review TO miovision_admins;
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE miovision_api.open_issues_review TO miovision_api_bot;

REVOKE ALL ON TABLE miovision_api.open_issues_review FROM bdit_humans;
GRANT SELECT ON TABLE miovision_api.open_issues_review TO bdit_humans;

REVOKE ALL ON TABLE miovision_api.open_issues_review FROM ckousin;
GRANT UPDATE ON TABLE miovision_api.open_issues_review TO ckousin;
