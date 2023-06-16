# Centreline Conflation with Posted Speed Limit File April 2019


*Updating the Posted Speed Limit Shapefile for the City of Toronto to include current posted speed limits at each location and historical posted speed limits*

## Table of Contents
1. [Description of Project Problem](#description-of-project-problem)
2. [Methodology](#Methodology)
3. [Unmatched Bylaws](#unmatched-bylaws)

## Description of Project Problem

We were given a file of approximately 8000 descriptions of locations on Toronto streets, 
and each location represented a change to the posted speed limit on a certain stretch or road. 
Speed limit changes are enacted through bylaws in the City of Toronto, so each record was from a bylaw.
Each record also contains and ID (the ID of the location/text description), bylaw number, 
a date for when the bylaw was passed, the original speed limit of the location, and the updated speed.

Our job is to use this file to update the current speed limit shapefile with the data from the file. 
The current speed limit shapefile is basically the Toronto centreline layer, that also contains a speed for each segment. 
It links to the Toronto centreline using and ID. We would like the layer to be current as of March 2019 
and to also show the history of the speed limit on each road segment.

## Methodology

The process for matching bylaw locations to centreline geometries has slowly been improved over the past 8-10 months. 
Tables in the DB can be matched to geometries via a python script (`geom_script.py`). 

The code to create the table that contains all of the bylaw information for the posted speed limits in Toronto can be found at the top of the `manual_updates.sql` file.

## Unmatched bylaws

First, we looked at `vz_safety_programs_staging.speed_reduction_final`. This is the final version of our original  matching speed limits to the centreline segments attempt. The segments matched in this file do not account for special case 1 or 2 (the bylaws are matched from one intersection to another). There was a lot of manual work done to create this table, so if a bylaw occured in our umatched table and in the `vz_safety_programs_staging.speed_reduction_final`, and the bylaw occurred between two intersections cleanly (i.e. did not say stuff like `100 metres north of <street name>`), then we matched the bylaw to the centreline segments it was matched to in `vz_safety_programs_staging.speed_reduction_final`. The code for this is in the `manual_text_updates.sql` file.

There are still some bylaw text inputs that cannot be matched. Out of 7917 entries, there are still 138 distinct ID's that cannot be matched. 
However, I know some of the "matched" records are incorrectly matched because they occur on circular shaped streets or because they are matched to multi-linestring geometry and are incorrectly matched from there. 
To my knowledge, there are up to 40 of these cases (not to mention other incorrectly matched bylaws that I do not have knowledge of because I have not gotten to the QA/QC process yet). 


Of the 138 unique unmatched ID's bylaw text inputs there are: 
- 4 that are on highways
- 8 that are on highway on/off-ramps
- 14 rows that occur on streets that have more than one street with the same name in the city
- 5 that are erroring out when being called by the SQL function with the error `invalid input syntax for type double precision`. 
This error is coming from situations where the wrong part of the text is being identified as the number of metres away from an intersection that the bylaw occurs at.
- 44 of these have the word `point` in the bylaw. This means they are a special case 1 or a special case 2 which are described in more detail [here](https://github.com/CityofToronto/bdit_vz_programs/tree/master/safety_zones/commuity_safety_zone_bylaws#special-cases)
- 18 cases where the two intersections being which the bylaw occurs have been identified, the distance between the two points in under 11 metres (and a line was drawn between them)
- 3 cases where intersections were identified but a line was not able to be drawn between the two intersections
- 3 cases (not including the Allen Rd case) that I am not sure how to solve that are listed in [this issue](https://github.com/CityofToronto/bdit_data-sources/issues/188). I am sure there are more of these, I just have not found them yet. 
