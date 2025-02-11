# Special Events in the City of Toronto
Archiving the special events happening around the City of Toronto for traffic impact analysis.
## Data Sources
Currently, the databases hosts data from two data sources: City of Toronto Open Data and Ticketmaster.  

Item|City of Toronto Open Data|Ticketmaster Discovery API| 
----|-------------------------|--------------------------|
Link|[Link](https://open.toronto.ca/dataset/festivals-events/)|[Link](http://developer.ticketmaster.com/products-and-docs/apis/discovery-api/v2/) 
Format|JSON|JSON
Historical Data|NO|NO
Content|event name, venue name, venue address, start date and time, end date and time, classification|event name,  venue name, venue address, start date and time, classification
	
## Schema
The schema and table relations produced by the scripts are as follows. (PK: primary key; FK: foreign key)


!['Special Events Schema'](img/schema.png)

## Files

### ticketMasterAPICall.py
Call Ticketmaster API, retrieve all venues in Toronto, call the API again with each venue_id for events happening at each venue.  
Updates tables **tm_venues**, **tm_events**, and **venues**  

### parseEventJSON.py / parseEventXML.py (Before Dec 2016, city open data feed was in xml format.)
Get the city's open data feed, parse the information to store in tables od_events and od_venues.  
Updates tables **od_venues**, **od_events**, and **venues**

### GenEventList.py
Get events from tables **od_events** and **tm_events**, delete extra records of duplicate events (based on time and name), group events with the same name, and store them in tables **event_details** and **event_groups**.

### AddressFunctions.py
Contains 3 utility functions:
1. format_address(address)  
	This function takes in an address and formats it to its short form and strips punctuation and redundant characters. 
	Example: Takes '118 O'Connor Street, Toronto, ON, Canada' and returns '118 OConnor St'
	If the address is not in the form of 'street number + street name' will return the input address.
	
2. geocode(address)  
	Send address to Google Geocoding API and returns the formatted full address(street number, street name, city, province, country, postal code), latitude and longitude tuple.
	
3. rev_geocode(coordinates)  
	Send the coordinates to Google Geocoding API and returns the address in both the short form and the formatted full form.
