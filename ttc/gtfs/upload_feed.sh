#!/bin/bash
set -e  # Exit on any error
cd $1
/usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.calendar(service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date) FROM 'calendar.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
/usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.calendar_dates(service_id, date_, exception_type) FROM 'calendar_dates.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
/usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.routes(route_id, agency_id, route_short_name, route_long_name, route_desc, route_type, route_url, route_color, route_text_color) FROM 'routes.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
/usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.shapes(shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled) FROM 'shapes.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
/usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.stop_times(trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled) FROM 'stop_times.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
/usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.stops(stop_id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding) FROM 'stops.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
/usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.trips(route_id, service_id, trip_id, trip_headsign, trip_short_name, direction_id, block_id, shape_id, bikes_allowed, wheelchair_accessible) FROM 'trips.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
rm -rf $1