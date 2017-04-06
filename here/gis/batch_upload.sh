#!/bin/bash
for f in *.shp
do 
	f_lower="$(echo ${f,,})"
	tablename="$(echo ${f_lower%.*}_16_1)"
	shp2pgsql -D -I -s 4326 $f here_gis.$tablename | psql -h 10.160.12.47 -d bigdata 
	psql -h 10.160.12.47 -d bigdata -c "ALTER TABLE here_gis.$tablename OWNER TO here_admins; SELECT gis.clip_to('here_gis','$tablename');"

done
