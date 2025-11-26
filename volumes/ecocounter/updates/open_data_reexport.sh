#need to recreate these 3 files:
#cycling_permanent_counts_15min_1994_2024.csv
#cycling_permanent_counts_15min_2024_2025.csv
#cycling_permanent_counts_15min_2025_2026.csv

#as airflow:
HOST=""

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

#as bigdata user:
cp /home/airflow/open_data/permanent-bike-counters/* /data/open_data/permanent-bike-counters