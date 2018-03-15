#!/bin/bash
rev=17_4
for f in *.shp
do 
	f_lower="$(echo ${f,,})"
	tablename="$(echo ${f_lower%.*}_$rev)"
	shp2pgsql -D -I -s 4326 -S $f here_gis.$tablename | psql -h 10.160.12.47 -d bigdata 
	psql -h 10.160.12.47 -d bigdata -c "ALTER TABLE here_gis.$tablename OWNER TO here_admins; SELECT here_gis.clip_to('$tablename', '$rev');"

done
psql -h 10.160.12.47 -d bigdata -c "SELECT here_gis.split_streets_att('$rev');"