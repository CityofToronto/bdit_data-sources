CREATE TABLE wys.mobile_sign_installations_dupes (LIKE wys.mobile_sign_installations);

ALTER TABLE wys.mobile_sign_installations_dupes ADD UNIQUE( location, from_street, to_street, direction, 
            installation_date, removal_date, new_sign_number, comments)