-- Table: gtfs.calendar_dates

-- DROP TABLE IF EXISTS gtfs.calendar_dates;

CREATE TABLE IF NOT EXISTS gtfs.calendar_dates
(
    service_id integer,
    date_ bigint,
    exception_type integer,
    feed_id integer,
    CONSTRAINT calendar_dates_feed_id_fkey FOREIGN KEY (feed_id)
    REFERENCES gtfs.feed_info (feed_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE
    NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gtfs.calendar_dates
OWNER TO dbadmin;

REVOKE ALL ON TABLE gtfs.calendar_dates FROM bdit_humans;
REVOKE ALL ON TABLE gtfs.calendar_dates FROM gtfs_bot;

GRANT SELECT ON TABLE gtfs.calendar_dates TO bdit_humans;

GRANT ALL ON TABLE gtfs.calendar_dates TO dbadmin;

GRANT INSERT, SELECT, UPDATE ON TABLE gtfs.calendar_dates TO gtfs_bot;

COMMENT ON TABLE gtfs.calendar_dates
IS 'Trigger is used to instead insert into calendar_dates_imp.';
-- Index: fki_calendar_dates_feed_id_fkey

-- DROP INDEX IF EXISTS gtfs.fki_calendar_dates_feed_id_fkey;

CREATE INDEX IF NOT EXISTS fki_calendar_dates_feed_id_fkey
ON gtfs.calendar_dates USING btree
(feed_id ASC NULLS LAST)
TABLESPACE pg_default;

-- Trigger: calendar_dates_insert

-- DROP TRIGGER IF EXISTS calendar_dates_insert ON gtfs.calendar_dates;

CREATE OR REPLACE TRIGGER calendar_dates_insert
BEFORE INSERT
ON gtfs.calendar_dates
FOR EACH ROW
EXECUTE FUNCTION gtfs.calendar_dates_insert();