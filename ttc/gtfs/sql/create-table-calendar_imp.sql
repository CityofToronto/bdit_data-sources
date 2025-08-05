-- Table: gtfs.calendar_imp

-- DROP TABLE IF EXISTS gtfs.calendar_imp;

CREATE TABLE IF NOT EXISTS gtfs.calendar_imp
(
    service_id smallint,
    monday boolean NOT NULL,
    tuesday boolean NOT NULL,
    wednesday boolean NOT NULL,
    thursday boolean NOT NULL,
    friday boolean NOT NULL,
    saturday boolean NOT NULL,
    sunday boolean NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    feed_id integer,
    CONSTRAINT calendar_service_feed_pkey UNIQUE (service_id, feed_id),
    CONSTRAINT calendar_imp_feed_id_fkey FOREIGN KEY (feed_id)
    REFERENCES gtfs.feed_info (feed_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE
    NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gtfs.calendar_imp
OWNER TO dbadmin;

REVOKE ALL ON TABLE gtfs.calendar_imp FROM bdit_humans;
REVOKE ALL ON TABLE gtfs.calendar_imp FROM gtfs_bot;

GRANT SELECT ON TABLE gtfs.calendar_imp TO bdit_humans;

GRANT ALL ON TABLE gtfs.calendar_imp TO dbadmin;

GRANT INSERT, SELECT, UPDATE ON TABLE gtfs.calendar_imp TO gtfs_bot;
-- Index: fki_calendar_imp_feed_id_fkey

-- DROP INDEX IF EXISTS gtfs.fki_calendar_imp_feed_id_fkey;

CREATE INDEX IF NOT EXISTS fki_calendar_imp_feed_id_fkey
ON gtfs.calendar_imp USING btree
(feed_id ASC NULLS LAST)
TABLESPACE pg_default;