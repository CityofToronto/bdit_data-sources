/* Create parent table for all school safety zone sheets
*/
CREATE TABLE vz_safety_programs_staging.school_safety_zone_raw_parent (
   	like vz_safety_programs_staging.school_safety_zone_2021_raw 
	including all
);
	
ALTER TABLE vz_safety_programs_staging.school_safety_zone_raw_parent
     OWNER to vz_admins;

GRANT ALL ON TABLE vz_safety_programs_staging.school_safety_zone_raw_parent TO bdit_humans;
GRANT ALL ON TABLE vz_safety_programs_staging.school_safety_zone_raw_parent TO vz_admins;
GRANT ALL ON TABLE vz_safety_programs_staging.school_safety_zone_raw_parent TO vz_api_bot;

/* Define the year-based tables as children of the above parent table
*/
ALTER TABLE vz_safety_programs_staging.school_safety_zone_2018_raw 
	INHERIT vz_safety_programs_staging.school_safety_zone_raw_parent;

ALTER TABLE vz_safety_programs_staging.school_safety_zone_2019_raw 
	INHERIT vz_safety_programs_staging.school_safety_zone_raw_parent;
	
ALTER TABLE vz_safety_programs_staging.school_safety_zone_2020_raw 
	INHERIT vz_safety_programs_staging.school_safety_zone_raw_parent;
	
ALTER TABLE vz_safety_programs_staging.school_safety_zone_2021_raw 
	INHERIT vz_safety_programs_staging.school_safety_zone_raw_parent;
