CREATE TABLE gwolofs.miovision_doublings (
    intersection_uid integer,
    classification_uid integer,
    dt date,
    this_week_volume integer,
    previous_week_volume integer,
    this_week_days smallint,
    previous_week_days smallint,
    CONSTRAINT mio_doubling_pkey PRIMARY KEY (intersection_uid, classification_uid, dt)
)

ALTER TABLE gwolofs.miovision_doublings OWNER TO gwolofs;

GRANT SELECT ON gwolofs.miovision_doublings TO miovision_admins, miovision_api_bot;