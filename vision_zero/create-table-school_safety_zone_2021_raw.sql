/* Create empty table to store data from 2021 SSZ Google Sheet
*/
CREATE TABLE vz_safety_programs_staging.school_safety_zone_2021_raw
(
    school_name text COLLATE pg_catalog."default",
    address text COLLATE pg_catalog."default",
    work_order_fb text COLLATE pg_catalog."default",
    work_order_wyss text COLLATE pg_catalog."default",
    locations_zone text COLLATE pg_catalog."default",
    final_sign_installation text COLLATE pg_catalog."default",
    locations_fb text COLLATE pg_catalog."default",
    locations_wyss text COLLATE pg_catalog."default"
);

ALTER TABLE vz_safety_programs_staging.school_safety_zone_2021_raw
    OWNER to vz_admins;

GRANT ALL ON TABLE vz_safety_programs_staging.school_safety_zone_2021_raw TO bdit_humans;

GRANT ALL ON TABLE vz_safety_programs_staging.school_safety_zone_2021_raw TO vz_admins;

GRANT ALL ON TABLE vz_safety_programs_staging.school_safety_zone_2021_raw TO vz_api_bot;
