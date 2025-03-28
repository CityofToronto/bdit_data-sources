-- Table: itsc_factors.issuetypes

-- DROP TABLE IF EXISTS itsc_factors.issuetypes;

CREATE TABLE IF NOT EXISTS itsc_factors.issuetypes
(
    divisionid integer,
    issuetype integer,
    issuetype_desc text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS itsc_factors.issuetypes OWNER TO congestion_admins;

REVOKE ALL ON TABLE itsc_factors.issuetypes FROM bdit_humans;
GRANT SELECT ON TABLE itsc_factors.issuetypes TO bdit_humans;

GRANT SELECT ON TABLE itsc_factors.issuetypes TO events_bot;

COMMENT ON TABLE itsc_factors.issuetypes
IS 'Values manually inputted by referencing ITS Central. 
May need to be updated occasionally with new values.';
