﻿CREATE TABLE miovision_api.api_log
(
  intersection_uid integer,
  start_date date,
  end_date date,
  date_added date
)
WITH (
  OIDS=FALSE
);
ALTER TABLE miovision_api_test.api_log
  OWNER TO rliu;