--DROP VIEW open_data.monthly_api_interactions;
CREATE OR REPLACE VIEW open_data.monthly_api_interactions AS
WITH api_usage AS (
    --specific resource interaction
    SELECT
        r.page_id,
        api.count,
        api.dt
    FROM open_data.od_api_usage AS api
    JOIN open_data.od_resources AS r USING (resource_id)
    
    UNION

    --page interaction
    SELECT
        p.page_id,
        api.count,
        api.dt
    FROM open_data.od_api_usage AS api
    JOIN open_data.od_pages AS p ON p.page_id = api.resource_id
)

SELECT
    api.page_id,
    date_trunc('month', api.dt)::date AS mnth,
    SUM(api.count) AS api_interactions
FROM api_usage AS api
GROUP BY
    api.page_id,
    mnth;

ALTER TABLE open_data.monthly_api_interactions OWNER TO od_admins;

GRANT SELECT ON TABLE open_data.monthly_api_interactions TO bdit_humans;

GRANT SELECT ON TABLE open_data.monthly_api_interactions TO ref_bot;
