/* Define the new table to store 2021 data as a child of the parent table.
   Uses the structure of the existing 2018 table.
*/
CREATE TABLE vz_safety_programs_staging.school_safety_zone_2021_raw (
   	LIKE vz_safety_programs_staging.school_safety_zone_2018_raw
	INCLUDING ALL
) INHERITS (vz_safety_programs_staging.school_safety_zone_raw_parent);
