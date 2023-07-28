-- Table: wys.mobile_sign_installations_dupes

-- DROP TABLE IF EXISTS wys.mobile_sign_installations_dupes;

CREATE TABLE IF NOT EXISTS wys.mobile_sign_installations_dupes (
    ward_no INTEGER,
    location TEXT,
    from_street TEXT,
    to_street TEXT,
    direction TEXT,
    installation_date DATE,
    removal_date DATE,
    new_sign_number TEXT,
    comments TEXT,
    work_order INTEGER,
    CONSTRAINT mobile_sign_installations_dupes_key UNIQUE (work_order)
);

ALTER TABLE IF EXISTS wys.mobile_sign_installations_dupes OWNER TO wys_admins;

GRANT SELECT ON TABLE wys.mobile_sign_installations_dupes TO bdit_humans;

GRANT ALL ON TABLE wys.mobile_sign_installations_dupes TO wys_admins;

GRANT ALL ON TABLE wys.mobile_sign_installations_dupes TO wys_bot;

COMMENT ON TABLE wys.mobile_sign_installations_dupes
IS 'A place to automate the storage of duplicate (sign_id, installation_date) instead of putting them in the mobile installations table until we can get the WYS maintainers to fix the spreadsheets.';