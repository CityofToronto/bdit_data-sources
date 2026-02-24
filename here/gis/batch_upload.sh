#!/bin/bash

tablename="$(echo ${f_lower%.*}_$rev)"

read -p "What is your bigdata username?" your_username
read -p "What is your bigdata password?" your_password
read -p "Which map version are you trying to import?" rev
read -p "What directory are the shapefiles in?" filename


for f in Adminbndy3 Streets Zlevels
do
        f_lower="$(echo ${f,,})"
        tablename="$(echo ${f_lower%.*}_$rev)"
        ogr2ogr -f "PostgreSQL" PG:"host=trans-bdit-db-prod0-rds-smkrfjrhhbft.cpdcqisgj1fj.ca-central-1.rds.amazonaws.com dbname=bigdata user=$your_username password=$your_password" -nlt PROMOTE_TO_MULTI  /vsizip/$filename/$f.shp -nln "here_gis.$tablename" -progress
        psql -h trans-bdit-db-prod0-rds-smkrfjrhhbft.cpdcqisgj1fj.ca-central-1.rds.amazonaws.com -d bigdata -U $your_username -c "ALTER TABLE here_gis.$tablename OWNER TO here_admins; SELECT here_gis.clip_to('$tablename', '$rev');"
        psql -h trans-bdit-db-prod0-rds-smkrfjrhhbft.cpdcqisgj1fj.ca-central-1.rds.amazonaws.com -d bigdata -U $your_username -c "SELECT here_gis.update_geom_column('$tablename');"
done

psql -h trans-bdit-db-prod0-rds-smkrfjrhhbft.cpdcqisgj1fj.ca-central-1.rds.amazonaws.com -d bigdata -U $your_username -c "SELECT here_gis.split_streets_att('$rev');"