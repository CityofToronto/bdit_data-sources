# GENERAL INFORMATION

## Keywords
travel time, congestion, buffer index, probe data, speed

## Title of Dataset:
HERE data

## Description: What is HERE data?

HERE data is travel time data provided by HERE Technologies from a mix of vehicle probes. We have a daily [automated airflow pipeline](https://github.com/CityofToronto/bdit_data-sources/blob/master/dags/pull_here.py) that pulls 5-min aggregated speed data for each link in the city from the here API. For streets classified collectors and above, we aggregate up to segments using the [congestion network](https://github.com/CityofToronto/bdit_congestion/tree/grid/congestion_grid) and produce [summary tables](https://github.com/CityofToronto/bdit_congestion/blob/data_aggregation/congestion_data_aggregation/sql/generate_segments_tti_weekly.sql) with indices such as Travel Time Index and Buffer Index. 

*Travel Time Index: is the ratio of the average travel time and free-flow speeds. For example, a TTI of 1.3 indicates a 20-minute free-flow trip requires 26 minutes.*

*Buffer Index: indicates the additional time for unexpected delays that commuters should consider along with average travel time. For example, if BI and average travel time are 20% and 10 minutes, then the buffer time would be 2 minutes. Since it is calculated by 95th percentile travel time, it represents almost all worst-case delay scenarios and assures travelers to be on-time 95 percent of all trips.*

This is the coverage of here links in the city of Toronto. (from `here_gis.streets_21_1`)
![image](https://user-images.githubusercontent.com/46324452/149184544-bbff447b-bba7-4585-aebf-d9fc65d21998.png)

This is the coverage of the congestion network. 
![image](https://user-images.githubusercontent.com/46324452/149438775-20360279-10ef-4963-8d6a-188e934bb0c2.png)

## What do we use HERE data for:
We use HERE data to:
- Calculate travel time for aggregated time periods of at least three weeks for any two locations in the City (which can be used to measure the effects of infrastructure improvements like bike lanes);
- Examine changes to the Travel Time Index over time (to see the effects of road closures or Provincially mandated lock downs); and,
- Determine the 85th percentile speed (to inform speed limit or enforcement initiatives).

# Types of HERE data products
HERE Technologies provides us with a few different datasets, such as:
- traffic analytics tables containing probe data for all Toronto streets
- traffic patterns tables containing aggregated speed information for all Toronto streets
- street attribute tables showing speed limits, street names and one way info for Toronto's streets
- many GIS layers of streets, intersections, points of interest, trails, rail corridors and water features.

## Date of data collection: 
> provide single date: suggested format YYYY-MM-DD

> range (start, end, interval): From YYYY-MM-DD HH:MM:SS to YYYY-MM-DD HH:MM:SS (inclusive)

> approximate date;

> ongoing? preset (mention the last date)

> additional information

> regular updates? 

> last update

> provide the most recent record's date time (only for check)


## Geographic location of data collection:
> provide latitude, longiute, or city/region, Province, Country
>(if an area {(xmin,ymin), (xmax,ymax)})

> Provide CRS/SRID (e.g. WGS84/EPSG:4326)


## Support for data collection
> Information about who supported the collection of the data (program/unit): 


# SHARING/ACCESS INFORMATION

## Restrictions placed on the data: 
> list any restrictions on data access (public data, internal data (team, unit, division, City, a specific group pf people))

## Internal access to the data
> any process to access the data (requires registration, a link, let someone know)

> provide the information of how internal team (Data& Analytics) can find the data:
server name, schema, database, version, admin, 



## Licenses placed on the data (Privacy concerns)
> provide the link to know licence

> if it's not a known licence, provide any concerns

## Limitation of the data
> You can also provide any limitation of data that yields to useability of the data

## Links to any use the data: 
> If this data was used in any other dashbord for analysis or mapping purposes

## Links to other publicly accessible locations of the data: 
> Provide the links (Please write about which portal they can find the data, so in cases where link changed, people can search)

## Links/relationships to supplimentary data sets:
> Like centreline for studies/collisions

## Was data derived from another source?
> If yes, list source(s): 

## Do you expect citation for this dataset: 


# DATA & FILE OVERVIEW

## File List: 
> list all files (or folders, as appropriate for dataset organization) contained in the dataset, with a brief description

> Relationship between files, if important: 

## Additional related data collected
> this could be like centreline for studies or here map for traveltimes. 

## Format of accessible data
> The final format and easy to access

## Available formats 
> what format the data was collected in

> Other formats that data is available: map, txt, geojson, excel, shapefile

## Are there multiple versions of the dataset?
> If yes, 
1. name of file(s) that was updated: 
2. Why was the file updated? 
3. When was the file updated? 


# METHODOLOGICAL INFORMATION

## Description of methods used for collection/generation of data: 
> include links or references to other documentation containing design or protocols used in data collection

## Methods for processing the data: 
> If the data is not the raw collected data, describe how the data were generated from the raw or collected data


## Instrument- or software-specific information needed to interpret the data:
> Provide information on how data can be interpreted baed on collection device/instrument and 

## Standards information, if appropriate: 

## Environmental/experimental conditions: 

## Describe any quality-assurance procedures performed on the data: 

## People involved with sample collection, processing, analysis and/or submission: 


# DATA-SPECIFIC INFORMATION FOR:
 [FILENAME]
> Repeat this section for each dataset, folder or file, as appropriate

## Number of variables: 

## Number of cases/rows: 

## Variable List: 
> Use a table format to list variable name(s), description(s), unit(s) and value labels as appropriate for each

## Missing data codes: 
> list code/symbol and definition

## Specialized formats or other abbreviations used:

## Contact Information:
Further inquiries about HERE data should be sent to transportationdata@toronto.ca.  
