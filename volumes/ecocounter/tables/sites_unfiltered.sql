CREATE TABLE ecocounter.sites_unfiltered (
    site_id numeric NOT NULL,
    site_description text COLLATE pg_catalog."default" NOT NULL,
    geom GEOMETRY (POINT, 4326),
    facility_description text COLLATE pg_catalog."default",
    notes text COLLATE pg_catalog."default",
    replaced_by_site_id numeric,
    validated boolean,
    centreline_id integer,
    first_active timestamp without time zone,
    last_active timestamp without time zone,
    date_decommissioned timestamp without time zone,
    counter character varying(16) COLLATE pg_catalog."default",
    linear_name_full text COLLATE pg_catalog."default",
    side_street text COLLATE pg_catalog."default",
    technology text COLLATE pg_catalog."default",
    CONSTRAINT sites_pkey PRIMARY KEY (site_id),
    CONSTRAINT sites_replaced_by_fkey FOREIGN KEY (replaced_by_site_id)
    REFERENCES ecocounter.sites_unfiltered (site_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE ecocounter.sites_unfiltered OWNER TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.sites_unfiltered FROM bdit_humans;
REVOKE ALL ON TABLE ecocounter.sites_unfiltered FROM ecocounter_bot;

GRANT ALL ON TABLE ecocounter.sites_unfiltered TO ecocounter_admins;
GRANT SELECT, INSERT ON TABLE ecocounter.sites_unfiltered TO ecocounter_bot;
GRANT SELECT ON ecocounter.sites_unfiltered TO ecocounter_data_detectives;

COMMENT ON TABLE ecocounter.sites_unfiltered IS E''
'CAUTION: Use VIEW `ecocounter.sites` which includes only sites verified by a human.'
'Sites represent separate installations, where each site may have one or more flow.';

COMMENT ON COLUMN ecocounter.sites_unfiltered.site_id
IS 'unique site identifier used by ecocounter';

COMMENT ON COLUMN ecocounter.sites_unfiltered.facility_description
IS 'description of bike-specific infrastructure which the sensor is installed within';

COMMENT ON COLUMN ecocounter.sites_unfiltered.replaced_by_site_id IS E''
'Several sites had their sensors replaced and show up now as "new" sites though we should '
'ideally treat the data as continuous with the replaced site. This field indicates the site_id '
'of the new replacement site, if any.';

COMMENT ON COLUMN ecocounter.sites_unfiltered.centreline_id IS E''
'The nearest street centreline_id, noting that ecocounter sensors are only configured to count '
'bike-like objects on a portion of the roadway ie. cycletrack or multi-use-path. '
'Join using `JOIN gis_core.centreline_latest USING (centreline_id)`.';

COMMENT ON COLUMN ecocounter.sites_unfiltered.first_active IS E''
'First timestamp site_id appears in ecocounter.counts_unfiltered. '
'Updated using trigger with each insert on ecocounter.counts_unfiltered. ';

COMMENT ON COLUMN ecocounter.sites_unfiltered.last_active IS E''
'Last timestamp site_id appears in ecocounter.counts_unfiltered. '
'Updated using trigger with each insert on ecocounter.counts_unfiltered. ';

-- Trigger: audit_trigger_row

-- DROP TRIGGER IF EXISTS audit_trigger_row ON ecocounter.sites_unfiltered;

CREATE OR REPLACE TRIGGER audit_trigger_row
AFTER INSERT OR DELETE OR UPDATE 
ON ecocounter.sites_unfiltered
FOR EACH ROW
EXECUTE FUNCTION ecocounter.if_modified_func('true');

-- Trigger: audit_trigger_stm

-- DROP TRIGGER IF EXISTS audit_trigger_stm ON ecocounter.sites_unfiltered;

CREATE OR REPLACE TRIGGER audit_trigger_stm
AFTER TRUNCATE
ON ecocounter.sites_unfiltered
FOR EACH STATEMENT
EXECUTE FUNCTION ecocounter.if_modified_func('true');
