# Centreline Conflation with Posted Speed Limit File April 2019

The process for matching bylaw locations to centreline geometries has slowly been improved over the past 8-10 months. 
Tables in the DB can be matched to geometries via a python script (`geom_script.py`). 

The code to create the table that contains all of the bylaw information for the posted speed limits in Toronto can be found at the top of the `manual_updates.sql` file.

## Unmatched bylaws

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
