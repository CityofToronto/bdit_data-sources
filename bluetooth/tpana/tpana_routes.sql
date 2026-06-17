CREATE TABLE IF NOT EXISTS bluetooth.tpana_routes (
    route_id varchar, -- there are duplicates
    route_name varchar,
    short_name varchar,
    start_offset_m float,
    end_offset_m float,
    comments text,
    extra_info text,
    links text[], -- array of link ids
    additional_info text
);

COMMENT ON TABLE bluetooth.tpana_routes
IS 'Route details extracted from TPANA''s Equipment.xml received on 2026-06-02.';

ALTER TABLE bluetooth.tpana_routes OWNER TO bt_admins;

GRANT SELECT ON TABLE bluetooth.tpana_routes TO bdit_humans;
