-- Table: gtfs.calendar

-- DROP TABLE IF EXISTS gtfs.calendar;

CREATE TABLE IF NOT EXISTS gtfs.calendar
(
    service_id integer,
    monday integer,
    tuesday integer,
    wednesday integer,
    thursday integer,
    friday integer,
    saturday integer,
    sunday integer,
    start_date bigint,
    end_date bigint,
    feed_id integer,
    CONSTRAINT calendar_feed_id_fkey FOREIGN KEY (feed_id)
    REFERENCES gtfs.feed_info (feed_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE
    NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gtfs.calendar
OWNER TO dbadmin;

REVOKE ALL ON TABLE gtfs.calendar FROM bdit_humans;
REVOKE ALL ON TABLE gtfs.calendar FROM gtfs_bot;

GRANT SELECT ON TABLE gtfs.calendar TO bdit_humans;

GRANT ALL ON TABLE gtfs.calendar TO dbadmin;

GRANT INSERT, SELECT, UPDATE ON TABLE gtfs.calendar TO gtfs_bot;

COMMENT ON TABLE gtfs.calendar
IS 'Trigger is used to instead insert into calendar_imp.';
-- Index: fki_calendar_feed_id_fkey

-- DROP INDEX IF EXISTS gtfs.fki_calendar_feed_id_fkey;

CREATE INDEX IF NOT EXISTS fki_calendar_feed_id_fkey
ON gtfs.calendar USING btree
(feed_id ASC NULLS LAST)
TABLESPACE pg_default;

-- Trigger: calendar_insert

-- DROP TRIGGER IF EXISTS calendar_insert ON gtfs.calendar;

CREATE OR REPLACE TRIGGER calendar_insert
BEFORE INSERT
ON gtfs.calendar
FOR EACH ROW
EXECUTE FUNCTION gtfs.calendar_insert();