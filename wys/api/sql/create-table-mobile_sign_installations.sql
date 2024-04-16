-- Table: wys.mobile_sign_installations

-- DROP TABLE wys.mobile_sign_installations;

CREATE TABLE IF NOT EXISTS wys.mobile_sign_installations
(
    ward_no integer NOT NULL,
    location text COLLATE pg_catalog."default",
    from_street text COLLATE pg_catalog."default",
    to_street text COLLATE pg_catalog."default",
    direction text COLLATE pg_catalog."default",
    installation_date date NOT NULL,
    removal_date date,
    new_sign_number text COLLATE pg_catalog."default" NOT NULL,
    comments text COLLATE pg_catalog."default",
    confirmed text COLLATE pg_catalog."default",
    id integer NOT NULL DEFAULT nextval('wys.mobile_sign_installations_id_seq'::regclass),
    work_order integer,
    CONSTRAINT mobile_sign_installations_pkey
    PRIMARY KEY (ward_no, installation_date, new_sign_number)
)
PARTITION BY LIST (ward_no);

ALTER TABLE wys.mobile_sign_installations OWNER TO wys_admins;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE wys.mobile_sign_installations TO bdit_humans;
GRANT ALL ON TABLE wys.mobile_sign_installations TO wys_bot;

-- Trigger: audit_trigger_row
CREATE OR REPLACE TRIGGER audit_trigger_row
AFTER INSERT OR DELETE OR UPDATE ON wys.mobile_sign_installations
FOR EACH ROW EXECUTE FUNCTION wys.if_modified_func('true');

-- Trigger: audit_trigger_stm
CREATE OR REPLACE TRIGGER audit_trigger_stm
AFTER TRUNCATE ON wys.mobile_sign_installations
FOR EACH STATEMENT EXECUTE FUNCTION wys.if_modified_func('true');

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
