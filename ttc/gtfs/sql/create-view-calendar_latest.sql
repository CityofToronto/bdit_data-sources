-- View: gtfs.calendar_latest

-- DROP VIEW gtfs.calendar_latest;

CREATE OR REPLACE VIEW gtfs.calendar_latest
AS
SELECT DISTINCT ON (calendar_imp.feed_id)
    calendar_imp.service_id,
    calendar_imp.monday,
    calendar_imp.tuesday,
    calendar_imp.wednesday,
    calendar_imp.thursday,
    calendar_imp.friday,
    calendar_imp.saturday,
    calendar_imp.sunday,
    calendar_imp.start_date,
    calendar_imp.end_date,
    calendar_imp.feed_id
FROM gtfs.calendar_imp
ORDER BY calendar_imp.feed_id ASC, calendar_imp.end_date DESC;

ALTER TABLE gtfs.calendar_latest
OWNER TO gtfs_admins;

GRANT SELECT ON TABLE gtfs.calendar_latest TO bdit_humans;
GRANT ALL ON TABLE gtfs.calendar_latest TO gtfs_admins;

