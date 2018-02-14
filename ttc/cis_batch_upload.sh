#!/bin/bash
#For each compressed data file f
for f in *.csv.gz
do 
    #Unzip and pipe to a psql \COPY command
	gunzip -c $f |	psql -h 10.160.12.47 -d bigdata -c "\COPY ttc.cis(message_datetime, route, run, vehicle, latitude, longitude) FROM STDIN WITH (FORMAT csv, HEADER TRUE, DELIMITER '|');"
done