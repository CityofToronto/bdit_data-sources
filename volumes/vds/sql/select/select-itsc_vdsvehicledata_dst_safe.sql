SELECT
    divisionid,
    vdsid,
    TIMEZONE('UTC', timestamputc) AT TIME ZONE 'EST5EDT' AS dt, --convert timestamp (without timezone) at UTC to EDT/EST
    lane,
    sensoroccupancyds,
    round(speedkmhdiv100 / 100, 1) AS speed_kmh,
    round(lengthmeterdiv100 / 100, 1) AS length_meter
FROM public.vdsvehicledata
WHERE
    divisionid = 2 --8001 and 8046 have only null values for speed/length/occupancy
    --omits the 3rd hour of the day to avoid inserting duplicates when tz changes from EDT->EST
    AND ((
		timestamputc >= TIMEZONE('UTC', {start}::timestamptz)
    	AND timestamputc < TIMEZONE('UTC', {start}::timestamptz + interval '2 HOURS')
	) OR (
		timestamputc >= TIMEZONE('UTC', {start}::timestamptz + interval '3 HOURS')
		AND timestamputc < TIMEZONE('UTC', {start}::timestamptz + interval '1 DAY')
	))
    