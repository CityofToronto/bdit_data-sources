CREATE TABLE wys.locations
(
  api_id integer NOT NULL,
  address text,
  sign_name text,
  dir text,
  CONSTRAINT locations_api_id_pkey PRIMARY KEY (api_id)
)
WITH (
  OIDS=FALSE
);