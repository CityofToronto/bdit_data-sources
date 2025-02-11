# Introduction

This folder contains information on data sources related to road closures and special events. 

## [road_closures](./road_closures/)
This folder contains [Road Disruption Activity Reporting System (RoDARS)](https://www.toronto.ca/services-payments/streets-parking-transportation/road-restrictions-closures/road-disruption-activity-reporting-system-rodars/) which is pulled daily from ITS Central by the [`rodars_pull` DAG](../dags/rodars_pull.py).  
The describes permitted road construction and event related road closures with data dating back to ~2012, with more accurate data starting in mid-2024 (RoDARS "New"). 

## [special_events](./special_events/)
This folder contains an outdated (2017) attempt at archiving special events happening around the City of Toronto for traffic impact analysis from two data sources: City of Toronto Open Data and Ticketmaster. 