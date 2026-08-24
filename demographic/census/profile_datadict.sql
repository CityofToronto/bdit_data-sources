DROP TABLE IF EXISTS census2021.profile_datadict;

CREATE TABLE census2021.profile_datadict
(
    id int,
    description varchar(500)
);

ALTER TABLE census2021.profile_datadict OWNER to rbahreh;

GRANT ALL ON TABLE census2021.profile_datadict TO census_admins;

GRANT SELECT ON TABLE census2021.profile_datadict TO bdit_humans;

COMMENT ON TABLE census2021.profile_datadict IS 'Data dictionary for census2021.profile_data';