-- Table: gwolofs.ecocounter_rename

DROP TABLE IF EXISTS gwolofs.ecocounter_rename;

CREATE TABLE IF NOT EXISTS gwolofs.ecocounter_rename
(
    site_description text COLLATE pg_catalog."default" NOT NULL,
    site_description_new text COLLATE pg_catalog."default",
    CONSTRAINT ecocounter_rename_pkey PRIMARY KEY (site_description)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.ecocounter_rename
OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.ecocounter_rename FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.ecocounter_rename TO bdit_humans;

GRANT ALL ON TABLE gwolofs.ecocounter_rename TO gwolofs;

INSERT INTO gwolofs.ecocounter_rename (
VALUES
    ('Bloor St W between Huron St & Spadina Ave', 'Bloor St W, west of Huron St'),
    ('Bloor St W between Huron St & Spadina Ave (retired)', 'Bloor St W, west of Huron St (retired)'),
    ('Bloor St W, between Palmerston & Markham', 'Bloor St W, east of Palmerston Blvd'),
    ('Bloor St W, between Palmerston & Markham (retired)', 'Bloor St W, east of Palmerston Blvd (retired)'),
    ('Bloor St W, East of Old Mill Trail', 'Bloor St W, east of Old Mill Trl'),
    ('YorkU - Keele St, North of Four Winds Dr', 'Keele St, north of Four Winds Dr (multi-use path)'),
    ('YorkU - Murray Ross Pkwy, North of Shoreham Dr', 'Murray Ross Pkwy, north of Shoreham Dr (multi-use path)'),
    ('YorkU - Murray Ross Pkwy, West of Evelyn Wiggins Dr', 'Murray Ross Pkwy, west of Evelyn Wiggins Dr (multi-use path)'),
    ('Yonge St, North of Macpherson Ave', 'Yonge St, north of Macpherson Ave'),
    ('Yonge St, South of Davisville Ave', 'Yonge St, south of Davisville Ave'),
    ('Yonge St, North of St Clair Ave', 'Yonge St, north of St Clair Ave'),
    ('Sherbourne St, North of Gerrard St (retired)', 'Sherbourne St, north of Gerrard St (retired)'),
    ('Sherbourne St, North of Gerrard St', 'Sherbourne St, north of Gerrard St'),
    ('Yonge St, North of Bloor St', 'Yonge St, north of Bloor St'),
    ('Bloor St E, West of Castle Frank Rd', 'Bloor St E, west of Castle Frank Rd'),
    ('Bloor St E, West of Castle Frank Rd (retired)', 'Bloor St E, west of Castle Frank Rd (retired)'),
    ('Sherbourne St, North of Wellesley St E', 'Sherbourne St, north of Wellesley St E'),
    ('Yonge St, North of Davenport Rd', 'Yonge St, north of Davenport Rd'),
    ('Sherbourne St, North of Wellesley St E (retired)', 'Sherbourne St, north of Wellesley St E (retired)'),
    ('YorkU - Evelyn Wiggins Dr, North of Murray Ross Pkwy', 'Evelyn Wiggins Dr, north of Murray Ross Pkwy (two-way cycle track)'),
    ('Bloor St W at Oakmount Rd, Display Counter', 'Bloor St W, west of Oakmount Rd'),
    ('Steeles Ave E and McCowan Rd WB', 'Steeles Ave E, at McCowan Rd'),
    ('Steeles Ave E and Midland Ave EB', 'Steeles Ave E, east of Midland Ave'),
    ('Steeles Ave E and Midland Ave WB', 'Steeles Ave E, east of Midland Ave'),
    ('Multi-use path south-east of Keele & Sheppard', 'Keele St, south of Sheppard Ave W (multi-use path)'),
    ('Multi-use path south of Sheppard Ave at Sentinel Rd', 'Sheppard Ave W, west of Sentinel Rd (multi-use path)'),
    ('Millwood - NE of Donlands', 'Millwood Rd, north of Pape Ave/Donlands Ave'),
    ('Millwood - NW of Pape', 'Millwood Rd, north of Pape Ave/Donlands Ave')
);

UPDATE ecocounter.open_data_15min_counts AS og
SET site_description = ecocounter_rename.site_description_new
FROM gwolofs.ecocounter_rename
WHERE og.site_description = ecocounter_rename.site_description;

UPDATE ecocounter.open_data_daily_counts AS og
SET site_description = ecocounter_rename.site_description_new
FROM gwolofs.ecocounter_rename
WHERE og.site_description = ecocounter_rename.site_description;

UPDATE ecocounter.sites_unfiltered AS og
SET site_description = ecocounter_rename.site_description_new
FROM gwolofs.ecocounter_rename
WHERE og.site_description = ecocounter_rename.site_description;

UPDATE open_data.cycling_permanent_counts_locations AS og
SET location_name = ecocounter_rename.site_description_new
FROM gwolofs.ecocounter_rename
WHERE og.location_name = ecocounter_rename.site_description;

UPDATE ecocounter.manual_counts_info AS og
SET ecocounter_location = ecocounter_rename.site_description_new
FROM gwolofs.ecocounter_rename
WHERE og.ecocounter_location = ecocounter_rename.site_description;

DROP TABLE gwolofs.ecocounter_rename;
