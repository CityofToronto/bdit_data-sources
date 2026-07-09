CREATE VIEW open_data.monthly_file_clicks AS
SELECT
    click.mnth,
    r.page_id,
    SUM(click.clicks) AS clicks
FROM
    open_data.od_file_clicks AS click
    JOIN open_data.od_resources AS r ON click.package_name = r.page_id
GROUP BY
    click.mnth,
    r.page_id
ORDER BY
    clicks DESC;

ALTER TABLE open_data.monthly_file_clicks owner TO od_admins;

GRANT SELECT ON TABLE open_data.monthly_file_clicks TO bdit_humans;

GRANT SELECT ON TABLE open_data.monthly_file_clicks TO ref_bot;
