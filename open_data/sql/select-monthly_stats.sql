SELECT
    pg.page_id,
    pg.views AS pg_views,
    api.api_interactions,
    click.clicks AS file_clicks
FROM open_data.od_page_views AS pg 
LEFT JOIN open_data.monthly_api_interactions AS api USING (page_id, mnth)
LEFT JOIN open_data.monthly_file_clicks AS click USING (page_id, mnth)
WHERE mnth = {mnth}
ORDER BY pg.views + api.api_interactions + click.clicks DESC;
