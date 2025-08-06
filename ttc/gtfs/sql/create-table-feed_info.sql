-- Table: gtfs.feed_info

-- DROP TABLE IF EXISTS gtfs.feed_info;

CREATE TABLE IF NOT EXISTS gtfs.feed_info
(
    feed_id integer NOT NULL DEFAULT nextval('gtfs.feed_info_feed_id_seq'::regclass),
    insert_date timestamp with time zone,
    CONSTRAINT feed_info_pkey PRIMARY KEY (feed_id),
    CONSTRAINT insert_date_unique UNIQUE (insert_date)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gtfs.feed_info
OWNER TO gtfs_admins;

REVOKE ALL ON TABLE gtfs.feed_info FROM bdit_humans;
REVOKE ALL ON TABLE gtfs.feed_info FROM gtfs_bot;

GRANT SELECT ON TABLE gtfs.feed_info TO bdit_humans;

GRANT ALL ON TABLE gtfs.feed_info TO gtfs_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE gtfs.feed_info TO gtfs_bot;
-- Index: fki_feed_info_feed_id_fkey

-- DROP INDEX IF EXISTS gtfs.fki_feed_info_feed_id_fkey;

CREATE INDEX IF NOT EXISTS fki_feed_info_feed_id_fkey
ON gtfs.feed_info USING btree
(feed_id ASC NULLS LAST)
TABLESPACE pg_default;