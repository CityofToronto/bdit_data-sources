#/bin/bash
#Downloads 30 min volume data for the sensor list in sensors.list and saves them as csv.

#TODO change these variables. Or make them arguments to the code
year="2017"
month="5"

#grab username, pw, and baseurl from file
#Dangerous practice, since this executes the contents of auth.txt
#https://askubuntu.com/a/367180/334823
source auth.txt

#reads each sensorid from the newline separated sensors.list file
while read sensorid
do
    url="$baseurl?year=$year&month=$month&reportType=min_30&sensorName=$sensorid"
    curl -u $username:$password $url -o $sensorid.csv

done < sensors.list
echo "Finished downloading data for month $year-$month"