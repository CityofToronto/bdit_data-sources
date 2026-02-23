-- Table: gtfs.calendar_dates_imp

-- DROP TABLE IF EXISTS gtfs.calendar_dates_imp;

CREATE TABLE IF NOT EXISTS gtfs.calendar_dates_imp
(
    service_id smallint NOT NULL,
    date_ date NOT NULL,
    exception_type smallint NOT NULL,
    feed_id integer,
    CONSTRAINT calendar_dates_imp_unique UNIQUE (service_id, date_, feed_id),
    CONSTRAINT calendar_dates_imp_feed_id_fkey FOREIGN KEY (feed_id)
    REFERENCES gtfs.feed_info (feed_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE
    NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gtfs.calendar_dates_imp
OWNER TO gtfs_admins;

REVOKE ALL ON TABLE gtfs.calendar_dates_imp FROM bdit_humans;
REVOKE ALL ON TABLE gtfs.calendar_dates_imp FROM gtfs_bot;

GRANT SELECT ON TABLE gtfs.calendar_dates_imp TO bdit_humans;

GRANT ALL ON TABLE gtfs.calendar_dates_imp TO gtfs_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE gtfs.calendar_dates_imp TO gtfs_bot;
-- Index: fki_calendar_dates_imp_feed_id_fkey

-- DROP INDEX IF EXISTS gtfs.fki_calendar_dates_imp_feed_id_fkey;

CREATE INDEX IF NOT EXISTS fki_calendar_dates_imp_feed_id_fkey
ON gtfs.calendar_dates_imp USING btree
(feed_id ASC NULLS LAST)
TABLESPACE pg_default;