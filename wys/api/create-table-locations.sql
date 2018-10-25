CREATE TABLE wys.locations
(
  api_id integer NOT NULL,
  sign_name text,
  address text,
  serial_num integer,
  dir text,
  CONSTRAINT locations_api_id_pkey PRIMARY KEY (api_id)
)
WITH (
  OIDS=FALSE
);