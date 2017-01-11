# Special Events in the City of Toronto
Archiving the special events happening around the City of Toronto for traffic impact analysis.
## Data Sources
1. City of Toronto Open Data
	Data can be retrieved [here](http://www1.toronto.ca/wps/portal/contentonly?vgnextoid=8b0689fe9c18b210VgnVCM1000003dd60f89RCRD&vgnextchannel=1a66e03bb8d1e310VgnVCM10000071d60f89RCRD)
	Data format: JSON
	Have historical archive: NO
	
2. Ticketmaster Discovery API
	Documentation of the API [here](http://developer.ticketmaster.com/products-and-docs/apis/discovery/v2/)
	Data format: JSON
	Have historical archive: NO
	
## Schema
The schema and table relations produced by the scripts are as follows.
!['Special Events Schema'](img/Special Events Schema.png)

## Files

### ticketMasterAPICall.py
Calls Ticketmaster API and retrieve all venues in Toronto and then calls the API again with each venue_id for events happening at each venue.
Updates tables **tm_venues** and **tm_events** 

### parseEventJSON.py / parseEventXML.py (Before Dec 2016, city open data feed was in xml format.)
Get the city's open data feed and parses the information to store in tables od_events and od_venues.
Updates tables **od_venues**, **od_events**, **venues**

### GenEventList.py
Get events from tables **od_events** and **tm_events**, delete extra records of duplicate events (based on time and name), group events with the same name, and store them in tables **event_details** and **event_groups**.

### AddressFunctions.py
Contains 3 utility functions:
1. FormatAddress(address)
	This function takes in an address and formats it to its short form and strips punctuation and redundant characters. 
	Example: Takes '118 O'Connor Street, Toronto, ON, Canada' and returns '118 OConnor St'
	If the address is not in the form of 'street number + street name' will return the input address.
	
2. geocode(address)
	Send address to Google Geocoding API and returns the formatted full address(street number, street name, city, province, country, postal code), latitude and longitude tuple.
	
3. rev_geocode(coordinates)
	Send the coordinates to Google Geocoding API and returns the address in both the short form and the formatted full form.