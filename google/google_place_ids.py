#Part 1: get required fields from the licenced business table in postgres to create a search string

import psycopg2
from credentials import *
import googlemaps
import pprint
import time
import json
import urllib.request



try:
    connection = get_connection()
    cursor = connection.cursor()
    select_query = """SELECT licence_no, client_name, licence_address_line1, licence_address_line3 FROM lic_business LIMIT 5"""
    cursor.execute(select_query)
    records = cursor.fetchall()
    print("establishing connection and writing records")
except (Exception, psycopg2.Error) as error:
    print("Error while getting data", error)

# Part 2: Create a search string from the table query and pass it to find_place in google. Final result is a list of data set with licence number, place_id, name and address from google
data = []
gmaps = googlemaps.Client(key = api_key())
for row in records:
            lic_no = (row[0])
            name = (row[1])
            address = (row[2])
            postcode = (row[3])
            lookup = (name+' '+address+' '+postcode)
            my_fields = ['place_id', 'name', 'formatted_address']
            place_result = (gmaps.find_place(lookup, 'textquery', fields=my_fields, location_bias='ipbias', language=None))
            print(place_result)
            try:
                alist = place_result["candidates"][0]
                gname = alist.get("name")
                gplace_id = alist.get("place_id")
                gaddress = alist.get("formatted_address")
                i = ([lic_no, gname, gplace_id, gaddress])
                data.append(i)
                
            except IndexError:
                pass

# part 3: inserts the retrieved data into an empty postgres table
insert_query = """INSERT INTO test_1 (lic_no, gname, gplace_id, gaddress) VALUES (%s,%s,%s,%s)"""
for item in data:
    cursor.executemany(insert_query, (item,))
    connection.commit()
close_connection(connection)
# Now commiting changes with #315 to check whether it goes to the required place
