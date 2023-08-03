-- Table: wys.mobile_sign_installations

-- DROP TABLE wys.mobile_sign_installations;

CREATE TABLE wys.mobile_sign_installations -- noqa: PRS
(
    ward_no integer,
    location text COLLATE pg_catalog."default",
    from_street text COLLATE pg_catalog."default",
    to_street text COLLATE pg_catalog."default",
    direction text COLLATE pg_catalog."default",
    installation_date date,
    removal_date date,
    new_sign_number text COLLATE pg_catalog."default",
    comments text COLLATE pg_catalog."default",
    id serial,
    work_order integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE wys.mobile_sign_installations OWNER TO wys_admins;

GRANT SELECT, REFERENCES, TRIGGER ON TABLE wys.mobile_sign_installations TO bdit_humans;

GRANT ALL ON TABLE wys.mobile_sign_installations TO jchew;

GRANT DELETE ON TABLE wys.mobile_sign_installations TO rdumas;

GRANT ALL ON TABLE wys.mobile_sign_installations TO wys_bot;