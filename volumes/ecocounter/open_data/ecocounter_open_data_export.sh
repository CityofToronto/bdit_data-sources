HOST=trans-bdit-db-prod0-rds-smkrfjrhhbft.cpdcqisgj1fj.ca-central-1.rds.amazonaws.com
USER=gwolofs
#dest_path="/data/open_data/permanent-bike-counters"
dest_path="/data/home/gwolofs/open_data/permanent-bike-counters"
YR1=(1994 2024)
YR2=(2024 2025)

cd ~
rm -f -r $dest_path/*.csv
mkdir "$dest_path/"

for ((i=0; i<${#YR1[@]}; i++)) do
    /usr/bin/psql -h $HOST -U $USER -d bigdata -c \
                "SELECT location_dir_id, datetime_bin, bin_volume
                FROM open_data.cycling_permanent_counts_15min
                WHERE
                    datetime_bin >= to_date(${YR1[i]}::text, 'yyyy')
                    AND datetime_bin < LEAST(date_trunc('month', now()), to_date((${YR2[i]})::text, 'yyyy'))
                    ORDER BY location_dir_id, datetime_bin;" \
                --csv -o "$dest_path/cycling_permanent_counts_15min_${YR1[i]}_${YR2[i]}.csv"
done

#need to export this on 
#pandoc -V geometry:margin=1in \
#            -o $dest_path/cycling_permanent_counts_readme.pdf \
#            /data/home/gwolofs/bdit_data-sources/volumes/open_data/sql/cycling_permanent_counts_readme.md

#as gwolofs
#grant permission for bigdata to read from my home folder.
setfacl -R -m u:bigdata:rx $dest_path

pbrun su - bigdata
#rm /data/open_data/permanent-bike-counters/*
cp -r $dest_path/*.csv /data/open_data/permanent-bike-counters
cp -r $dest_path/*.pdf /data/open_data/permanent-bike-counters

cd /data/open_data/permanent-bike-counters
wc -l ./*