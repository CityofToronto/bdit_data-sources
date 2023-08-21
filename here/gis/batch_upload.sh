#!/bin/bash
rev=23_1
filename = 2EAM231E0N2E000AACU8.tar.gz
tablename="$(echo ${f_lower%.*}_$rev)"

for f in Adminbndy3 Streets Zlevels
do 
	f_lower="$(echo ${f,,})"
	tablename="$(echo ${f_lower%.*}_$rev)"
    ogr2ogr -f "PostgreSQL" PG:"host=10.160.8.132 dbname=bigdata user=your_username password=your_password" -nlt PROMOTE_TO_MULTI /vsitar/$filename/${f_lower}.shp -nln "here_gis.$tablename" -progress
	psql -h 10.160.12.47 -d bigdata -c "ALTER TABLE here_gis.$tablename OWNER TO here_admins; SELECT here_gis.clip_to('$tablename', '$rev');"

done

psql -h 10.160.12.47 -d bigdata -c "SELECT here_gis.split_streets_att('$rev');"


