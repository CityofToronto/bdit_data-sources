DROP TABLE IF EXISTS census2021.profile_data;
CREATE TABLE census2021.profile_data 
    (
        alt_geo_code bigint,
        var_id int,
        val varchar(35)
    );

CREATE INDEX census2021_profile_data_idx1
            ON census2021.profile_data USING btree(alt_geo_code);

CREATE INDEX census2021_profile_data_idx2
            ON census2021.profile_data USING btree(var_id);

ALTER TABLE census2021.profile_data OWNER to rbahreh;

GRANT ALL ON TABLE census2021.profile_data TO census_admins;

GRANT SELECT ON TABLE census2021.profile_data TO bdit_humans;

COMMENT ON TABLE census2021.profile_data IS 'Raw census 2021 profiles at the dissemination area-scale.';