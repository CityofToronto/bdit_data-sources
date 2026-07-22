
create materialized view  open_data_staging.ksi_vz AS 

select rec_id, accnb as accident_number, 
stname1||' '||streetype1||' '||dir1 as street1,
stname2||' '||streetype2||' '||dir2 as street2,
year::int as accident_year,
accdate::date as accident_date,
to_timestamp(acctime, 'HH24MI')::time  as accident_time,
accdate +  to_timestamp(acctime, 'HH24MI')::time  as datetime,
injury::int,
CASE
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) <= 4) THEN '0 to 4'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 5 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 9) THEN '5 to 9'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 10 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 14) THEN '10 to 14'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 15 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 19) THEN '15 to 19'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 20 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 24) THEN '20 to 24'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 25 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 29) THEN '25 to 29'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 30 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 34) THEN '30 to 34'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 35 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 39) THEN '35 to 39'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 40 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 44) THEN '40 to 44'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 45 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 49) THEN '45 to 49'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 50 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 54) THEN '50 to 54'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 55 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 59) THEN '55 to 59'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 60 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 64) THEN '60 to 64'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 65 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 69) THEN '65 to 69'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 70 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 74) THEN '70 to 74'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 75 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 79) THEN '75 to 79'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 80 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 84) THEN '80 to 84'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 85 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 89) THEN '85 to 89'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 90 AND case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end <= 94) THEN '90 to 94'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) >= 95) THEN 'Over 95'
        WHEN ((case when INVAGE ~ '[^0-9]' THEN null else INVAGE::int end) IS NULL) THEN 'Unknown'
        ELSE 'Unknown'
    END AS "INVAGE",
invtype as involvement_type,
light as light_condition,
visible as visibility_condition,
rdsfcond as road_surface_condition,
longitude,
latitude,
wardnum
FROM collisions.acc 
where CONCAT(date_part('year', accdate), ':', accnb) in (
SELECT distinct CONCAT(date_part('year', accdate), ':', accnb) as collision_id
from collisions.acc 
where injury in ('3','4'));

ALTER TABLE IF EXISTS open_data_staging.ksi_vz
    OWNER TO collisions_bot;

COMMENT ON MATERIALIZED VIEW open_data_staging.ksi_vz
    IS 'Staging materialized view for open_data.ksi_vz, refreshes daily through airflow DAG ksi_opendata on ec2.';

GRANT SELECT ON TABLE open_data_staging.ksi_vz TO collision_admins;
GRANT ALL ON TABLE open_data_staging.ksi_vz TO collisions_bot;