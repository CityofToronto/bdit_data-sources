-- Table: wys.mobile_sign_installations

-- DROP TABLE wys.mobile_sign_installations;

CREATE TABLE wys.mobile_sign_installations -- noqa: PRS
(
    ward_no integer,
    location text COLLATE pg_catalog."default",
    from_street text COLLATE pg_catalog."default",
    to_street text COLLATE pg_catalog."default",
    direction text COLLATE pg_catalog."default",
    installation_date date,
    removal_date date,
    new_sign_number text COLLATE pg_catalog."default",
    comments text COLLATE pg_catalog."default",
    id integer NOT NULL DEFAULT nextval('wys.mobile_sign_installations_id_seq'::regclass),
    work_order integer
    CONSTRAINT mobile_sign_installations_pkey
    PRIMARY KEY (ward_no, installation_date, new_sign_number)
)
PARTITION BY LIST (ward_no)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE wys.mobile_sign_installations OWNER TO wys_admins;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE wys.mobile_sign_installations TO bdit_humans;
GRANT ALL ON TABLE wys.mobile_sign_installations TO wys_bot;

--create partitions with permissions: 
DO $do$
DECLARE
	ward INTEGER;
    ward_table TEXT;
BEGIN
	FOR ward IN 1..25 LOOP
        ward_table := 'ward_'||ward::text;
        EXECUTE FORMAT($$
                       CREATE TABLE %s PARTITION OF wys.mobile_sign_installations
                       FOR VALUES IN (%s::integer);
                       ALTER TABLE wys.%s OWNER TO wys_admins;
                       GRANT SELECT, REFERENCES, TRIGGER ON TABLE wys.%s TO bdit_humans;
                       GRANT ALL ON TABLE wys.%s TO wys_bot;
                       $$, ward_table, ward, ward_table, ward_table, ward_table);
	END LOOP;
END;
$do$ LANGUAGE plpgsql