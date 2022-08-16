---------------------------------------------------------------------------------------------------------------------------------------------------
--CHECK 400-series Highways
---------------------------------------------------------------------------------------------------------------------------------------------------
SELECT * FROM rbahreh.col_orig_abbr WHERE stname1 ~* '\DQEW*\D*' OR stname1 ~* '\D400*\D*' OR stname1 ~* '\D427*\D*';--1516
SELECT * FROM rbahreh.col_orig_abbr WHERE stname1 ~* '\yhwy\y' OR stname1 ~* '\yhighway\y';--2786 

--ALTER TABLE rbahreh.col_orig_abbr DROP COLUMN hwy_flag; 
ALTER TABLE rbahreh.col_orig_abbr ADD COLUMN hwy_flag BOOLEAN; 

UPDATE rbahreh.col_orig_abbr 
SET hwy_flag = CASE
WHEN stname1 ~* '\DQEW*\D*' OR stname1 ~* '\D400*\D*' OR stname1 ~* '\D427*\D*' OR stname1 ~* '\yhwy\y' OR stname1 ~* '\yhighway\y' OR  
 stname2 ~* '\DQEW*\D*' OR stname2 ~* '\D400*\D*' OR stname2 ~* '\D427*\D*' OR stname2 ~* '\yhwy\y' OR stname2 ~* '\yhighway\y'  OR
 stname3 ~* '\DQEW*\D*' OR stname3 ~* '\D400*\D*' OR stname3 ~* '\D427*\D*' OR stname3 ~* '\yhwy\y' OR stname3 ~* '\yhighway\y' THEN TRUE
ELSE FALSE
END;

SELECT Count(*) FROM rbahreh.col_orig_abbr WHERE hwy_flag is TRUE;--13315
-- We can only be sure that a collision is highway related,
--beacuse sometimes a complete address is in the stnames

---------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------
--Occured on highway
---------------------------------------------------------------------------------------------------------------------------------------------------
---ONE HIGHWAY: have highway in stname1
ALTER TABLE rbahreh.col_orig_abbr ADD COLUMN hwy_add1_flag BOOLEAN; 

UPDATE rbahreh.col_orig_abbr 
SET hwy_add1_flag = CASE 
WHEN stname1 ~* '\DQEW*\D*' OR stname1 ~* '\D400*\D*' 
OR stname1 ~* '\D427*\D*' OR stname1 ~* '\yhwy\y' 
OR stname1 ~* '\yhighway\y' THEN TRUE
ELSE FALSE
END;
---------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------
--CREATE A COLUMN FOR LOCATION_TYPE AND LOCATION_CLASS
---------------------------------------------------------------------------------------------------------------------------------------------------
--EXPLORE
SELECT COUNT(DISTINCT collision_no) FROM rbahreh.col_orig_abbr WHERE LOCATION_TYPE IS NOT NULL;--496508/619,162=80%
SELECT COUNT(DISTINCT collision_no) FROM rbahreh.col_orig_abbr WHERE LOCATION_CLASS IS NOT NULL;--345258 of total =55%
SELECT COUNT(DISTINCT collision_no) FROM rbahreh.col_orig_abbr WHERE LOCATION_TYPE IS NOT NULL AND LOCATION_CLASS IS NOT NULL;--281901 of total 45%
SELECT COUNT(DISTINCT collision_no) FROM rbahreh.col_orig_abbr WHERE LOCATION_TYPE IS NULL AND LOCATION_CLASS IS NOT NULL;--63357 of total =10% of the 20% without the location_type, have locations_class

***************************************************************************************************************************************************
/****NOTE:***
Based on Location_Type, 80% of collisions have identified location type at which the collision occured
According to corresponding Location_Class, 45% of them are validated, i.e. has bothe location_type and location_class.

Therefore we can update the location_type with location_class which is a validated version.

We are left with 20% of total collisions that do not have location_type, but 10% of them have verified location_class.

So in total, only 10% of them happened at unknown type of location, meaning do not have neither of location_class and location_type*/
***************************************************************************************************************************************************

ALTER TABLE rbahreh.col_orig_abbr ADD COLUMN loc_type_class TEXT;

UPDATE rbahreh.col_orig_abbr
SET loc_type_class= CASE
WHEN LOCATION_TYPE IS NULL AND LOCATION_CLASS IS NULL THEN 'RR' --Requires Review
WHEN LOCATION_TYPE IS NOT NULL AND LOCATION_CLASS IS NOT NULL THEN LOCATION_CLASS
WHEN LOCATION_TYPE IS NULL AND LOCATION_CLASS IS NOT NULL THEN LOCATION_CLASS
WHEN LOCATION_TYPE IS NOT NULL AND LOCATION_CLASS IS NULL THEN LOCATION_TYPE
END;--6 seconds

SELECT COUNT(DISTINCT collision_no) FROM rbahreh.col_orig_abbr WHERE loc_type_class='RR';--59297 (9.6%)
---------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------
--Address TYPE
---------------------------------------------------------------------------------------------------------------------------------------------------
--Absolute address
ALTER TABLE rbahreh.col_orig_abbr ADD COLUMN address_type text;

UPDATE rbahreh.col_orig_abbr
SET address_type = CASE 
WHEN collision_add1 ~* '^\d+\D+' AND hwy_add1_flag IS FALSE THEN 'absolute_address_1'
WHEN collision_add1 ~* '^\d+\D+' AND hwy_add1_flag IS TRUE THEN 'absolute_address_1_hwy' -- there is a possibility that it's a false positive (a highway identified as an absolute address)
WHEN collision_add2 ~* '^\d+\D+' AND hwy_flag IS FALSE THEN 'absolute_address_2'
WHEN collision_add2 ~* '^\d+\D+' AND hwy_flag IS TRUE THEN 'absolute_address_2_hwy_poss'
WHEN collision_add3 ~* '^\d+\D+' AND hwy_flag IS FALSE THEN 'absolute_address_3'
WHEN collision_add3 ~* '^\d+\D+' AND hwy_flag IS TRUE THEN 'absolute_address_3_hwy_poss'
ELSE 'not_absolute'
END;
---------------------------------------------------------------------------------------------------------------------------------------------------