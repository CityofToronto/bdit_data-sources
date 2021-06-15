# Bluetooth reader's status
The table `detectors_history_corrected` is prepared by referencing the tables that are available in cnangini schema,  latest tables that are received from our contact person from bliptrack and cross checked with route points from `all_analyses` table in our bluetooth schema. The following tables were used from cnangini schema. 

-  `detectors_validated`
-  `detector_history`
-  `detectors`

The table that was received from bliptrack is `mohan.latest_bt_locations_jj`

The table that was used from bluetooth schema is `all_analyses` field `route_points`. This field contains json objects and three key:value pairs `name`, `latitude`, `longitude`were used as a reference. These key:value pairs are there for start point and end point of each route or `analysis_id`. It was used to cross check the information in creating the history of intersections with bluetooth detectors.  

These tables contain common fields such as `BT Unit #` in `detectors_validated` corresponds to `ZONE` in `mohan.latest_bt_locations_jj`. The common corresponding fields in each set of table is listed below.

|latest_bt_locations_jj|detectors_validated|detectors_history_corrected|all_analysis|remarks|
|----------------------|-------------------|---------------------------|------------|-------|
|ZONE|BT Unit #|bt_id|
||detector_name|read_name|name|
|LOCATION_DATE |date_installed|date_start||The earliest date for the detector at the location among the tables.|
|LAST_STATUS_UPDATE||last_data_received_date||The latest date for a particular detector among the tables|.
||date_removed|date_end||This date is assumed to be the date when a bt reader is physically removed from an intersection. The latest date found among the tables that is used.|
|LAT|latitude|lat|latitude|
|LNG|longitude|lon|longitude|

The following status are assigned to each reader based on the information available in all tables. 
- online (readers that are online as per `mohan.latest_bt_locations_jj` and whose details match with the rest of the tables)
- offline (readers that are offline as per mohan.latest_bt_locations_jj with matching details in other tables)
- decommissioned ((readers that are in the `detector_history` table but not in the `latest_bt_locations_jj` and whose `date_removed` or `date_end` is populated ))


Apart from the above three status following four categories are populated to maintain the history of locations of the detectors/reporting points. Either detectors have been installed in these locations in the past and now moved away or these locations are/were used in creating and reporting routes. 

- added (these are the locations where names are inconsistent for example a reader with bt_id 4035 has a name E in one table and E4035 in another. Despite being at the same intersection at Gardiner & Parliament. Therefore, these two names could refer to the same reader. Thus to distinguish one got the status `added` and another is `offline` )
  
- duplicate (`duplicate` does not have a bt_id but based on its location and/or the reader name, there are more than one at a given intersection. The one that has got less details is assigned as `duplicate`)
  
- moved (`moved` has a bt_id but the same bt_id existed in a different location in the past. For example bt_id 5331 was at Gerrard & University. But the same bt_id also is at Lawrence & Mt Pleasant and is `offline` as per latest table. Thus 5331 has two entries with status `offline` and `moved`.
   
- routepoint name (does not have a bt_id. It is a name given to a point along a route. Could there be a detector at those points ? most likely not)

This table is a working progress and as new details arise, it has to be updated. 
