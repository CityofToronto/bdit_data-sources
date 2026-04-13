#!/bin/bash

tablename="$(echo ${f_lower%.*}_$rev)"

read -p "What is your bigdata username?" your_username
read -p "What is your bigdata password?" your_password
read -p "Enter bigdata's hostname" host_name
read -p "Which map version are you trying to import?" rev
read -p "What directory are the shapefiles in?" filename


for f in Adminbndy3 Streets Zlevels
do
        f_lower="$(echo ${f,,})"
        tablename="$(echo ${f_lower%.*}_$rev)"
        ogr2ogr -f "PostgreSQL" PG:"$host_name dbname=bigdata user=$your_username password=$your_password" -nlt PROMOTE_TO_MULTI  /vsizip/$filename/$f.shp -nln "here_gis.$tablename" -progress
        psql -h $host_name -d bigdata -U $your_username -c "ALTER TABLE here_gis.$tablename OWNER TO here_admins; SELECT here_gis.clip_to('$tablename', '$rev');"
        psql -h $host_name -d bigdata -U $your_username -c "SELECT here_gis.update_geom_column('$tablename');"
done

psql -h $host_name -d bigdata -U $your_username -c "SELECT here_gis.split_streets_att('$rev');"