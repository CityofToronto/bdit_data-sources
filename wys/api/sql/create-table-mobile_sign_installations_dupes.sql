-- Table: wys.mobile_sign_installations_dupes

-- DROP TABLE IF EXISTS wys.mobile_sign_installations_dupes;

CREATE TABLE IF NOT EXISTS wys.mobile_sign_installations_dupes --noqa: PRS
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
    work_order integer,
    CONSTRAINT mobile_sign_installations_dup_location_from_street_to_stree_key UNIQUE NULLS NOT DISTINCT (location, from_street, to_street, direction, installation_date, removal_date, new_sign_number, comments)
);

ALTER TABLE IF EXISTS wys.mobile_sign_installations_dupes OWNER TO wys_admins;

GRANT SELECT ON TABLE wys.mobile_sign_installations_dupes TO bdit_humans;

GRANT ALL ON TABLE wys.mobile_sign_installations_dupes TO wys_admins;

GRANT ALL ON TABLE wys.mobile_sign_installations_dupes TO wys_bot;

COMMENT ON TABLE wys.mobile_sign_installations_dupes
IS 'A place to automate the storage of duplicate (sign_id, installation_date) instead of putting them in the mobile installations table until we can get the WYS maintainers to fix the spreadsheets.';