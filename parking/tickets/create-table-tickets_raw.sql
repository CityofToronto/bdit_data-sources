CREATE TABLE parking.tickets_raw (
	ticket_uid serial NOT NULL,
	tag_number_masked text,
	date_of_infraction text,
	infraction_code text,
	infraction_description text,
	set_fine_amount text,
	time_of_infraction text,
	location1 text,
	location2 text,
	location3 text,
	location4 text,
	province text,
	source_file text
);

GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes_15min TO bdit_humans;


CREATE INDEX parking_tickets_uid_idx
  ON parking.tickets_raw
  USING btree
  (ticket_uid);
