CREATE TABLE wys.mobile_sign_installations_dupes
(
    ward_no integer,
    location text,
    from_street text,
    to_street text,
    direction text,
    installation_date date,
    removal_date date,
    new_sign_number text,
    comments text,
    CONSTRAINT mobile_sign_installations_dup_location_from_street_to_stree_key UNIQUE (location, from_street, to_street, direction, installation_date, removal_date, new_sign_number, comments)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE wys.mobile_sign_installations_dupes
    OWNER to wys_admins;


COMMENT ON TABLE wys.mobile_sign_installations_dupes
    IS 'A place to automate the storage of duplicate (sign_id, installation_date) instead of putting them in the mobile installations table until we can get the WYS maintainers to fix the spreadsheets.';