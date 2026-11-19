#need to recreate these 5 files:
#cycling_permanent_counts_15min_1994_2024.csv
#cycling_permanent_counts_15min_2024_2025.csv
#cycling_permanent_counts_15min_2025_2026.csv
#cycling_permanent_counts_15min_2026_2027.csv

#/cycling_permanent_counts_daily.csv


#as airflow:
HOST=""

### 15min

/usr/bin/psql -h $HOST -U ecocounter_bot -d bigdata -c \
    "SELECT location_dir_id, datetime_bin, bin_volume
    FROM open_data.cycling_permanent_counts_15min
    WHERE
        datetime_bin >= to_date(1994::text, 'yyyy')
        AND datetime_bin < LEAST(date_trunc('month', now()), to_date((2024)::text, 'yyyy'))
    ORDER BY location_dir_id, datetime_bin;" \
    --csv -o "/home/airflow/open_data/permanent-bike-counters/cycling_permanent_counts_15min_1994_2024.csv"
    
/usr/bin/psql -h $HOST -U ecocounter_bot -d bigdata -c \
    "SELECT location_dir_id, datetime_bin, bin_volume
    FROM open_data.cycling_permanent_counts_15min
    WHERE
        datetime_bin >= to_date(2024::text, 'yyyy')
        AND datetime_bin < LEAST(date_trunc('month', now()), to_date((2025)::text, 'yyyy'))
    ORDER BY location_dir_id, datetime_bin;" \
    --csv -o "/home/airflow/open_data/permanent-bike-counters/cycling_permanent_counts_15min_2024_2025.csv"

/usr/bin/psql -h $HOST -U ecocounter_bot -d bigdata -c \
    "SELECT location_dir_id, datetime_bin, bin_volume
    FROM open_data.cycling_permanent_counts_15min
    WHERE
        datetime_bin >= to_date(2025::text, 'yyyy')
        AND datetime_bin < LEAST(date_trunc('month', now()), to_date((2026)::text, 'yyyy'))
    ORDER BY location_dir_id, datetime_bin;" \
    --csv -o "/home/airflow/open_data/permanent-bike-counters/cycling_permanent_counts_15min_2025_2026.csv"

/usr/bin/psql -h $HOST -U ecocounter_bot -d bigdata -c \
    "SELECT location_dir_id, datetime_bin, bin_volume
    FROM open_data.cycling_permanent_counts_15min
    WHERE
        datetime_bin >= to_date(2026::text, 'yyyy')
        AND datetime_bin < LEAST(date_trunc('month', now()), to_date((2027)::text, 'yyyy'))
    ORDER BY location_dir_id, datetime_bin;" \
    --csv -o "/home/airflow/open_data/permanent-bike-counters/cycling_permanent_counts_15min_2026_2027.csv"

### Daily

/usr/bin/psql -h $HOST -U ecocounter_bot -d bigdata -c \
    "SELECT
         location_dir_id, location_name, direction, linear_name_full,
         side_street, dt, daily_volume
     FROM open_data.cycling_permanent_counts_daily
         WHERE dt < LEAST(date_trunc('month', now()))
     ORDER BY location_dir_id, dt;" \
    --csv -o "/home/airflow/open_data/permanent-bike-counters/cycling_permanent_counts_daily.csv"


/usr/bin/psql -h $HOST -U ecocounter_bot -d bigdata -c \
                    "SELECT location_dir_id, location_name, direction, linear_name_full, side_street,
                        bidirectional, longitude, latitude, centreline_id, bin_size, latest_calibration_study,
                        first_active, last_active, date_decommissioned, technology
                    FROM open_data.cycling_permanent_counts_locations
                    ORDER BY location_dir_id;" \
                    --csv -o "/home/airflow/open_data/permanent-bike-counters/cycling_permanent_counts_locations.csv"

/usr/bin/psql -h $HOST -U ecocounter_bot -d bigdata -c \
        "SELECT featurecollection FROM open_data.cycling_permanent_counts_locations_geojson;" \
        --tuples-only -o "/home/airflow/open_data/permanent-bike-counters/cycling_permanent_counts_locations.geojson"

pandoc -V geometry:margin=1in -o "/home/airflow/open_data/permanent-bike-counters/cycling_permanent_counts_readme.pdf" "/data/home/gwolofs/bdit_data-sources/ecocounter/volumes/open_data/sql/cycling_permanent_counts_readme.md"

#as bigdata user:
#cp /home/airflow/open_data/permanent-bike-counters/* /data/open_data/permanent-bike-counters