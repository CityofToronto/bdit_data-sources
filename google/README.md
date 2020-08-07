# Business_Busyness
 
<h1>Pulling Google place ids</h1>

The goal of this script is to search the google database for all the businesses licenced with the City of Toronto using a find place request.
Google Places API is used to pull each business ID, business name and address as listed in google, and write that data in a new blank database along with business licence number issued by the City.
 
 API key is stored in a seperate module named credentials to make sure that it remains hidden from the script. Therefore, user needs to create a seperate .py file with API key and import that while this script is run.
 
psycopg2 is used to read and write the postgres tables. 

Google places API can return the results in either json or xml format, this script uses json format so json is also imported.

For this script to work, an empty table with the required fields should be created beforehand in the postgres database. This script just inserts the pulled data into the respective fields. 

pprint is optional. It is necessary just to test whether the pulled data (intermediate output) is in the correct format. pprint makes the output easy to read.

The script mainly has three parts:

First part establishes connection to the postgres table (in this case the table name is "lic_business" where existing licenced business database is located.
It then selects the licence number, name and complete address including postcode from the data table. It uses the name, address and postcode for each business to create a search field for that particular business. In the script the number of rows that is pulled is limited to 5 for the test purpose. When running in reality, this limit has to be removed. 


Second part, passes this search field to query google using places API find_place request.
Once the place is found in google database, it returns the business name, ID and address in google and append it with the City of Toronto Issued licence number and creates a list of all found businesses.

If any business that exist in the City database is not found in the Google database, the script skips those businesses.
 
The third part of the script then insert the retrieved data into the blank postgres table that is created (in this script "test_1") to insert the retrieved data with predetermined fields. The id field in the test_1 table is unique. Thus this script will produce an error message if run twice on the same table.


<h1>Pulling popular times and wait times data from google</h1>

The goal of this script is to pull the populartimes and wait times data for the businesses licenced with the City of Toronto where the business has got the google id.
Populartime data is a dynamic data. 

Populartimes and waittimes data are not available through google API as of August 2020. To get this data a python library named populartimes is used. This library is available to clone or download from https://github.com/m-wrzr/populartimes

API key is used to pull the populartime, waittimes and other details of the business licence number issued by the City. It uses two parameters - API key and google id. 
 
 API key is stored in a seperate module named credentials to make sure that it remains hidden from the script. Therefore, user needs to create a seperate .py file with API key and import that while this script is run.
 
 The data that is pulled are of two types. Static type and Dynamic type. Static data do not change by hour for example current_popularity and time spent. (however whether the data is static or not needs to be checked once data is pulled periodically in future)
 
 Dynamic data on the other hand is updated every hour. Dynamic data include popularity by hour, and wait time by hour
 
psycopg2 is used to read and write the postgres tables. 

populartimes library returns the data in json format so json is also imported.

For this script to work, two empty tables (one for static data and another for dynamic data) with the required fields should be created beforehand in the postgres database. This script just inserts the pulled data into the respective fields. 

pprint is optional. It is necessary just to test whether the pulled data (intermediate output) is in the correct format. pprint makes the output easy to read.

The script mainly has four parts:

First part establishes connection to the postgres table where existing licenced business database with google id is located.
It then retrieves the google id for each business from the database table which is used along with API key to search and pull the populartimes details form google. 


Second part, passes this google id to query google using populartimes.get_id(api_key, ID) method to retrieve the details about the place.
For each ID, it returns the popularity data in json format.

Not all businesses have popularity data. If any business that exist in the City database is not found in the Google popularity database, the script skips those businesses and search for next one.
 
Third part, Some businesses have popularity data but no time_wait data. This script will enter [null]  values to time wait data if time wait data does not exist in the google database.

The fourth part of the script then inserts the data in the list into the 2 blank postgres tables. Static data is pushed into static_data table and popularity data is pushed into populartimes table.

In static_data table there is one row for each google id. However in populartimes data, there is one row for each hour of the day, for each day of the week for both populartimes and wait times. Since this is a dynamic data, current date and data acquisition time also is pushed into the database. Therefore for each business, there will be 7*24 = 168 rows. 

Enumerate function is used to retrieve the hour of the day which is basically the list index and zip function is used to zip the populartimes and wait times together so that it becomes easy or iterator to pull the data efficiently at the same time. 

 
As this populartimes library uses the Google Places Web Service, where each API call over a monthly budget is priced. The API call is SKU'd as "Find Current Place" with additional Data SKUs (Basic Data, Contact Data, Atmosphere Data). As of August 2020, you can make almost 5000 calls with the alloted monthly budget of $200. For more information check https://developers.google.com/places/web-service/usage-and-billing and https://cloud.google.com/maps-platform/pricing/sheet/#places.
