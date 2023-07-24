DROP MATERIALIZED VIEW open_data.wys_stationary_locations;
CREATE MATERIALIZED VIEW open_data.wys_stationary_locations AS

SELECT 
    ss.sign_id,
    ss.address,
    ss.sign_name,
    ss.dir,
    ssc.schedule,
    ssc.min_speed,
    ssc.speed_limit,
    ssc.flash_speed,
    ssc.strobe_speed,
    ss.geom,
    wrd.area_short::int AS ward_no,
    CASE
        WHEN ss.prev_start IS NOT NULL THEN ss.start_date 
        ELSE MIN(agg.datetime_bin)
    END AS start_date,
    CASE
        WHEN MAX(agg.datetime_bin) < date_trunc('month'::text, now()) - interval '1 day'
            THEN LEAST(ss.next_start::timestamp without time zone, MAX(agg.datetime_bin))
    END AS end_date
FROM wys.stationary_signs AS ss
--JOIN wys.sign_schedules_list AS ssl USING (api_id) --never used in SELECT statement
LEFT JOIN wys.sign_schedules_clean AS ssc USING (schedule_name)
JOIN wys.speed_counts_agg_5kph AS agg USING (api_id)
LEFT JOIN gis.wards2018 AS wrd ON ST_Contains(wrd.wkb_geometry, ss.geom)
GROUP BY
    wrd.area_short,
    ss.sign_id, 
    ss.address,
    ss.sign_name,
    ss.dir,
    ssc.schedule,
    ssc.min_speed,
    ssc.speed_limit,
    ss.prev_start,
    ss.next_start,
    ss.start_date,
    ssc.flash_speed,
    ssc.strobe_speed,
    ss.geom;
GRANT SELECT ON TABLE open_data.wys_stationary_locations TO od_extract_svc;    
CREATE UNIQUE INDEX ON open_data.wys_stationary_locations (sign_id);

CREATE FUNCTION wys.refresh_od_mat_view()
RETURNS void
LANGUAGE 'sql'
COST 100
VOLATILE SECURITY DEFINER 
AS $BODY$
    REFRESH MATERIALIZED VIEW CONCURRENTLY open_data.wys_stationary_locations WITH DATA ;
$BODY$;
REVOKE EXECUTE ON FUNCTION wys.refresh_od_mat_view() FROM public;
GRANT EXECUTE ON FUNCTION wys.refresh_od_mat_view() TO wys_bot;
