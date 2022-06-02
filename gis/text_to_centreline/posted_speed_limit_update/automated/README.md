# Automated Posted Speed Limit Update (2019)

This folder contains information about the automation of creating a new layer with posted speed limits across the City of Toronto, created with data of the text descriptions of speed limit bylaws. 

The `bylaw_open_data_xml.py` file contains the python code that matches bylaw data from an `XML` file that was taken from [Open Data](https://open.toronto.ca/dataset/traffic-and-parking-by-law-schedules/) to centreline geometries. You can update this file to use it by updating where the `XML` file is pulled from, adding a `db.cfg` file to this folder (DO NOT commit it).

The `unmatched.csv` file contains all of the bylaws that were not matched. 

The `QC` folder contains information about the QC process that happened after the table of bylaw locations was created.

`final_posted_speed_layer.sql` contains the code that took the table outputted by `bylaw_open_data_xml.py` and converted it to a new posted speed limit layer. A bug that has been found is that it most likely does not work with multi-linestring bylaw geometries, since `ST_StartPoint`, `ST_EndPoint`, and `ST_LineLocatePoint` functions will not work with  multi-linestring types. The work around for bylaws that are multi-linestrings and occur between 2 intersections cleanly (i.e. do not occur `100 metres` from an intersection or something) would be to use the old centreline matching process for these bylaws only (the old centreline matching process is the one written in the 2018 posted speed limit documentation). I do not think adding this functionality to the process would be hard to implement. Another note is that this code takes over 2 hours to run. I think this could be made faster by optimizing the process by [not using CTEs](https://medium.com/@hakibenita/be-careful-with-cte-in-postgresql-fca5e24d2119). The current process in this file was assembled under a time crunch so its not the most amazing/optimized approach.
